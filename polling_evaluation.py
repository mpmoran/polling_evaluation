#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install realclearpolitics==1.4.0')


# In[2]:


get_ipython().system('rcp https://www.realclearpolitics.com/epolls/2020/president/tx/texas_trump_vs_biden-6818.html')


# In[3]:


get_ipython().system('ls -lh')


# In[4]:


from pyspark.sql import SparkSession


# In[5]:


def create_view(spark, name, sql_statement):
    """
    SparkSession, str, str -> None
    
    create view using sql_statement
    """
    spark.sql(sql_statement).createOrReplaceTempView(name)


def show_schema(spark, name, truncate):
    """
    SparkSession, str, bool -> None
    
    show schema of table or view
    """
    title = f'{name} schema'
    print(title)
    print('=' * len(title))

    spark.sql(f'''
        DESCRIBE {name}
    ''').show(truncate=truncate)


def show_peek(spark, name, num_rows, truncate):
    """
    SparkSession, str, int, bool -> None
    
    show num_rows from table or view
    """
    title = f'{name} peek'
    print(title)
    print('=' * len(title))
    
    spark.sql(f'''
        SELECT *
        FROM {name}
        LIMIT {str(num_rows)}
    ''').show(truncate=truncate)


# In[6]:


context = {
    'data_file': './texas_trump_vs_biden-6818.csv',
    'output_file': './output',
    'stages': [
        {
            'name': 'tx_pres_polls_stage1',
            'sql_statement': '''
                SELECT `Poll` as poll,
                       `Date` as date_range,
                       `Sample` as sample_size,
                       `MoE` as margin_of_error,
                       `Trump (R)` as trump,
                       `Biden (D)` as biden,
                       `Spread` as spread
                FROM tx_pres_polls_raw
            ''',
            'peek_rows': 10,
            'truncate_show': False,
        },
        {
            'name': 'tx_pres_results',
            'sql_statement': '''
                SELECT poll,
                       '2020-11-03' as election_date,
                       trump,
                       biden,
                       lower(split(spread, ' ', -1)[0]) as winner,
                       CASE WHEN split(spread, ' ', -1)[0] = 'Tie'
                            THEN 0.0
                            ELSE cast(split(spread, ' ', -1)[1] as double) END AS spread
                FROM tx_pres_polls_stage1
                WHERE poll = 'Final Results'
            ''',
            'peek_rows': 10,
            'truncate_show': False,
        },
        {
            'name': 'tx_pres_polls_stage2',
            'sql_statement': '''
                SELECT poll, 
                       date_range,
                       cast(split(sample_size, ' ', -1)[0] as int) as sample_size,
                       cast(margin_of_error as double) as margin_of_error,
                       trump,
                       biden,
                       lower(split(spread, ' ', -1)[0]) as winner,
                       split(spread, ' ', -1)[0] = 'Trump' as is_winner_correct,
                       CASE WHEN split(spread, ' ', -1)[0] = 'Tie'
                            THEN 0.0
                            ELSE cast(split(spread, ' ', -1)[1] as double) END AS spread
                FROM tx_pres_polls_stage1
                WHERE poll != 'Final Results'
            ''',
            'peek_rows': 10,
            'truncate_show': True,
            
        },
        {
            'name': 'tx_pres_polls_stage3',
            'sql_statement': '''
                SELECT *,
                       round(abs((SELECT trump from tx_pres_results) - trump), 1) as trump_distance,
                       round(abs((SELECT biden from tx_pres_results) - biden), 1) as biden_distance
                FROM tx_pres_polls_stage2
            ''',
            'peek_rows': 10,
            'truncate_show': True,
        },
        {
            'name': 'tx_pres_polls',
            'sql_statement': '''
                SELECT *,
                       round(abs(trump_distance + biden_distance), 1) as total_distance
                FROM tx_pres_polls_stage3
            ''',
            'peek_rows': 10,
            'truncate_show': True,
        },
    ],
}


# In[7]:


spark = (
    SparkSession.builder
    .master("local")
    .appName("polling_error")
    #.config("spark.some.config.option", "some-value")
    .getOrCreate()
)


# In[8]:


spark.read.csv(
    context['data_file'],
    header=True,
    inferSchema=True,
    enforceSchema=False,
    mode='FAILFAST'
).createOrReplaceTempView('tx_pres_polls_raw')

print('schema\n'
      '======')
spark.sql('''
    DESCRIBE tx_pres_polls_raw
''').show(truncate=False)

print('peek\n'
      '====')
spark.sql('''
    SELECT *
    FROM tx_pres_polls_raw
    LIMIT 10
''').show(truncate=False)


# In[9]:


for stage in context['stages']:
    create_view(spark, stage['name'], stage['sql_statement'])
    show_schema(spark, stage['name'], stage['truncate_show'])
    show_peek(spark, stage['name'], stage['peek_rows'], stage['truncate_show'])


# In[10]:


spark.catalog.cacheTable('tx_pres_polls')

spark.sql('''
    SELECT *
    FROM tx_pres_polls
    ORDER BY total_distance ASC
''').repartition(1).write.csv(context['output_file'],
                              mode='overwrite',
                              header=True)


# In[11]:


print('actual results\n'
      '==============')
spark.sql('''
    SELECT *
    FROM tx_pres_results
    LIMIT 10
''').show(truncate=False)


# In[12]:


print('top 5 polls\n'
      '===========')
spark.sql('''
    SELECT *
    FROM tx_pres_polls
    ORDER BY total_distance ASC
    LIMIT 5
''').toPandas()


# In[13]:


print('bottom 5 polls\n'
      '==============')
spark.sql('''
    SELECT *
    FROM tx_pres_polls
    ORDER BY total_distance DESC
    LIMIT 5
''').toPandas()

