local ops = require("mongodbOps") --加载mongodb操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、update、delete

local result = {}  -- 定义一个table
for k, v in pairs(row) do
	if k ~="ID" then -- 列名不为ID
		result[k] = v
	end
end
local id = row["ID"] --获取ID列的值
result["_id"] = id -- _id为MongoDB的主键标识

if action == "insert" then -- 新增事件
	ops.INSERT("t_user",result) -- 新增，第一个参数为collection名称，string类型；第二个参数为要修改的数据，talbe类型
elseif action == "delete" then -- 删除事件  -- 修改事件
	ops.DELETE("t_user",id) -- 删除，第一个参数为collection名称(string类型)，第二个参数为ID 
else -- 修改事件
	ops.UPDATE("t_user",id,result) -- 修改，第一个参数为collection名称，string类型；第二个参数为ID；第三个参数为要修改的数据，talbe类型
	-- ops.UPSERT("t_user",id,result) -- 修改或新增，第一个参数为collection名称，string类型；第二个参数为ID；第三个参数为要修改的数据，talbe类型
end 