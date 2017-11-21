# RocketMqInAction - RocketMq实战
------
    将RocketMQ的producer和listner封装成sender和consumer，力求简单实用。

## 发送消息
    封装类为：MqMessageSender，支持以下多种发送方式及异常处理
### 1. 同步和异步发送消息
### 2. 发送有序消息
### 3. 延迟发送消息
### 4. 异常处理
> * 失败重试


## 接收并消费消息
> 分为ConcurrentlyConsumer和OrderlyConsumer。
>> * 有序消息 -> 继承OrderlyConsumer
>> * 非有序消息 -> 可认为支持多线程, 继承ConcurrentlyConsumer
### 自动过滤重复消息，避免重复消费问题
> 具体实现：AbstractConsumer.isAlreadyConsumed(...)方法。
>> 利用redisson的RSetCache类的add特性来判断消息是否已经消费过了 
>>> true if value has been added. false if value already been in collection.
```java
isAlreadyConsumed = !FedisClient.getClient().getSetCache(hashKeyOfRedis).add(messageKey, 1L, TimeUnit.DAYS);
```
> 注意：FedisClient为未实现单例，用户得自行修复以上代码。