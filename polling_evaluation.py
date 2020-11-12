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


# In[7]:


df = (
    df.withColumnRenamed('Poll', 'poll')
      .withColumnRenamed('Date', 'date_range')
      .withColumnRenamed('Sample', 'sample_size')
      .withColumnRenamed('MoE', 'margin_of_error')
      .withColumnRenamed('Trump (R)', 'trump')
      .withColumnRenamed('Biden (D)', 'biden')
      .withColumnRenamed('Spread', 'spread')
)


# In[8]:


df.createOrReplaceTempView('tx_pres_polls_raw')


# In[9]:


spark.sql('''
DESCRIBE tx_pres_polls_raw
''').show(truncate=False)


# In[10]:


spark.sql('''
    SELECT *
    FROM tx_pres_polls_raw
    LIMIT 10
''').show(truncate=False)


# In[11]:


spark.sql('''
    SELECT poll,
           '2020-11-03' as election_date,
           trump,
           biden,
           lower(split(spread, ' ', -1)[0]) as winner,
           CASE WHEN split(spread, ' ', -1)[0] = 'Tie'
                THEN 0.0
                ELSE cast(split(spread, ' ', -1)[1] as double) END AS spread
    FROM tx_pres_polls_raw
    WHERE poll = 'Final Results'
''').createOrReplaceTempView('tx_pres_results')


# In[12]:


spark.sql('''
    DESCRIBE tx_pres_results
''').show()
spark.sql('''
    SELECT *
    FROM tx_pres_results
    LIMIT 10
''').show(truncate=False)


# In[13]:


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
    FROM tx_pres_polls_raw
    WHERE poll != 'Final Results'
''').createOrReplaceTempView('tx_pres_polls_stage1')


# In[14]:


spark.sql('''
    DESCRIBE tx_pres_polls_stage1
''').show()
spark.sql('''
    SELECT *
    FROM tx_pres_polls_stage1
    LIMIT 10
''').show()


# In[15]:


spark.sql('''
    SELECT *,
           (SELECT spread FROM tx_pres_results LIMIT 1) as results_spread,
           round(spread - (SELECT spread FROM tx_pres_results LIMIT 1), 1) as spread_diff
    FROM tx_pres_polls_stage1
''').createOrReplaceTempView('tx_pres_polls')


# In[16]:


spark.sql('''
    DESCRIBE tx_pres_polls
''').show()
spark.sql('''
    SELECT *
    FROM tx_pres_polls
    LIMIT 10
''').show()


# In[17]:


print('top 5 polls\n'
      '===========')
spark.sql('''
    SELECT *
    FROM tx_pres_polls
    WHERE is_winner_correct = true
    ORDER BY abs(spread_diff) ASC
    LIMIT 5
''').toPandas()


# In[18]:


print('bottom 5 polls\n'
      '==============')
spark.sql('''
    SELECT *
    FROM tx_pres_polls
    WHERE is_winner_correct = false
    ORDER BY abs(spread_diff) DESC
    LIMIT 5
''').toPandas()

