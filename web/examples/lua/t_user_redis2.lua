local ops = require("redisOps") --加载redis操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、updare、delete

if action == "insert" -- 只监听insert事件
then
    local key = row["USER_NAME"] --获取USER_NAME列的值
   
    local id = row["ID"] --获取ID列的值
    local userName = row["USER_NAME"] --获取USER_NAME列的值
    local password = row["PASSWORD"] --获取PASSWORD列的值
    local createTime = row["CREATE_TIME"] --获取CREATE_TIME列的值
    
    ops.HSET(key,"id",id) -- 对应Redis的HSET命令
    ops.HSET(key,"userName",userName) -- 对应Redis的HSET命令
    ops.HSET(key,"password",password) -- 对应Redis的HSET命令
    ops.HSET(key,"createTime",createTime) -- 对应Redis的HSET命令
end