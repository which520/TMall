package com.which.TMall.common.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtil {
  private val config = ConfigurationUtil("config.properties")
  private val host: String = config.getString("redis.host")
  private val port: Int = config.getInt("redis.port")
  private val jedisPoolConfig = new JedisPoolConfig()
  jedisPoolConfig.setMaxTotal(100)
  jedisPoolConfig.setMaxIdle(20)
  jedisPoolConfig.setMinIdle(20)
  jedisPoolConfig.setBlockWhenExhausted(true)
  jedisPoolConfig.setMaxWaitMillis(500)
  jedisPoolConfig.setTestOnBorrow(true)
  private val jedisPool = new JedisPool(jedisPoolConfig,host,port)
  def getJedisClient ={
    jedisPool.getResource
  }
}
