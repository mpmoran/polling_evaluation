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


spark = (
    SparkSession.builder
    .master("local")
    .appName("polling_error")
    #.config("spark.some.config.option", "some-value")
    .getOrCreate()
)


# In[6]:


df = spark.read.csv('texas_trump_vs_biden-6818.csv',
                    header=True,
                    inferSchema=True,
                    enforceSchema=False,
                    mode='FAILFAST')

df.createOrReplaceTempView('tx_pres_polls_raw')

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


# In[7]:


spark.sql('''
    SELECT `Poll` as poll,
           `Date` as date_range,
           `Sample` as sample_size,
           `MoE` as margin_of_error,
           `Trump (R)` as trump,
           `Biden (D)` as biden,
           `Spread` as spread
    FROM tx_pres_polls_raw
''').createOrReplaceTempView('tx_pres_polls_stage1')

print('schema\n'
      '======')
spark.sql('''
    DESCRIBE tx_pres_polls_stage1
''').show(truncate=False)

print('peek\n'
      '====')
spark.sql('''
    SELECT *
    FROM tx_pres_polls_stage1
    LIMIT 10
''').show(truncate=False)


# In[8]:


spark.sql('''
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
''').createOrReplaceTempView('tx_pres_results')

print('schema\n'
      '======')
spark.sql('''
    DESCRIBE tx_pres_results
''').show(truncate=False)

print('peek\n'
      '====')
spark.sql('''
    SELECT *
    FROM tx_pres_results
    LIMIT 10
''').show(truncate=False)


# In[9]:


spark.sql('''
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
''').createOrReplaceTempView('tx_pres_polls_stage2')

print('schema\n'
      '======')
spark.sql('''
    DESCRIBE tx_pres_polls_stage2
''').show(truncate=False)

print('peek\n'
      '====')
spark.sql('''
    SELECT *
    FROM tx_pres_polls_stage2
    LIMIT 10
''').show()


# In[10]:


spark.sql('''
    SELECT *,
           (SELECT spread FROM tx_pres_results LIMIT 1) as results_spread,
           CASE WHEN winner = 'trump'
                THEN round(spread - (SELECT spread FROM tx_pres_results LIMIT 1), 1)
                ELSE round(spread + (SELECT spread FROM tx_pres_results LIMIT 1), 1) END AS spread_error
    FROM tx_pres_polls_stage2
''').createOrReplaceTempView('tx_pres_polls')

print('schema\n'
      '======')
spark.sql('''
    DESCRIBE tx_pres_polls
''').show(truncate=False)

print('peek\n'
      '====')
spark.sql('''
    SELECT *
    FROM tx_pres_polls
    LIMIT 10
''').show()


# In[11]:


print('top 5 polls\n'
      '===========')
spark.sql('''
    SELECT *
    FROM tx_pres_polls
    ORDER BY abs(spread_error) ASC
    LIMIT 5
''').toPandas()


# In[12]:


print('bottom 5 polls\n'
      '==============')
spark.sql('''
    SELECT *
    FROM tx_pres_polls
    ORDER BY abs(spread_error) DESC
    LIMIT 5
''').toPandas()

