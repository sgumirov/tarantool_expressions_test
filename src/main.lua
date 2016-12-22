-- Please see README.md for description
-- CONFIG options --
MAX = 1000 --number of values in tables
DEPTH=4    --length of each expression
WIDTH=2600 --number of expressions. 
-- Note that number of dereferences is slightly more than DEPTH*WIDTH due to randomness of references in expressions.
-- For example 4*2600 gives ~10000 dereferences. 

debug=false --debug means use small data and print verbosely. True sets REPEATS option to 1.
inmem=false --false means use tarantool engine, true means use Lua tables
sharding=true --use sharding
batch=true --use q_insert to fill tables
wide=false --means execute expressions layer by layer (breadth traversal) 
deep=true  --means run fiber for each expression simultaneously (depth traversal)
REPEATS=10 --repeat dereferencing benchmark this time. Note that debug=true effectively sets this value to 1
-- END CONFIG --

Count=0
math.randomseed(os.time())

if deep == true and wide == true then
  print("FATAL: config options \"deep\" and \"wide\" cannot be both true at the same time. Exiting.")
  os.exit(-1)
end

if (deep == true) then --check for fibers
  if(require('fiber') == nil) then
    print("FATAL ERROR : `deep` option enabled with no LUA support for FIBER. Exiting.")
    os.exit(-1)
  end
end

if (sharding == true) then
  shard = require('shard')
  print("BATCH = "..tostring(batch))
end

if (wide==true) then
  f = require('fiber')
  if (f == nil) then 
    wide = false
    print("NO FIBERS (tarantool 1.6?) => disabling WIDE (now false)")
  else 
    require('execwait')
    print("FIBERS")
  end
else
  print("NO FIBERS")
end

fibers_count = 0

printf = function(s,...)
           return io.write(s:format(...))
end

--initialize and fill datamodel tables to use for dereferencing expressions
--this function is used from shard_main.lua, so should be on top
function init_db()
  local a,b,c,d,data
  a=box.schema.space.create('a')
  b=box.schema.space.create('b')
  c=box.schema.space.create('c')
  d=box.schema.space.create('d')
  data=box.schema.space.create('data')
  a:create_index('primary', {parts={1, 'STR'}})
  b:create_index('primary', {parts={1, 'STR'}})
  c:create_index('primary', {parts={1, 'STR'}})
  d:create_index('primary', {parts={1, 'STR'}})
  data:create_index('primary', {parts={1, 'STR'}})
  if sharding == false then
    for i = 1, MAX, 1 do
      box.space.a:insert{tostring(i), string.char(string.byte('a')+math.random(4)-1)}
      box.space.b:insert{tostring(i), string.char(string.byte('a')+math.random(4)-1)}
      box.space.c:insert{tostring(i), string.char(string.byte('a')+math.random(4)-1)}
      box.space.d:insert{tostring(i), tostring(math.random(MAX))}
      box.space['data']:insert{tostring(i), tostring(MAX-i+1)}
    end
  else
    --when sharding, will do it later
  end
end

if inmem == false then
  local b = require('box')
  if b ~= nil then 
    print("RUNNING INSIDE TARANTOOL")
    if shard ~= nil then
      --box is inited inside shard
      print("enabling sharding init..")
      require('shard-main')
      print("ENABLED")
    else
      print("NO SHARDING")
      sharding = false
      --init box
      box.cfg{
        wal_mode = "none",
      }
    end 
  end
end

local function init_expressions()
  local expr={}
  for i=1,WIDTH,1 do
    local z = {}
    for j=1,DEPTH,1 do
      z[j] = tostring(math.random(MAX))
    end
    expr[i] = z
  end
  return expr
end

local function init_expressions_test()
  local expr={}
  local MAX = 40
  local count = 1
  for i=1,10,1 do
    local z = {}
    for j=1,4,1 do
      z[j] = tostring(count)
      count = count + 1
    end
    expr[i] = z
  end
  return expr
end

local function init_inmem_test()
  local MAX=40
  local a={}
  local b={}
  local c={}
  local d={}
  local data={}
  for i = 1, MAX, 1 do
    a[i] = 'b'
    b[i] = 'c'
    c[i] = 'd'
    d[i] = tostring(i)
    data[i] = tostring(MAX-i+1)
  end
  return a,b,c,d,data
end

local function init_inmem()
  local a={}
  local b={}
  local c={}
  local d={}
  local data={}
  for i = 1, MAX, 1 do
    a[i] = string.char(string.byte('a')+math.random(4)-1)
    b[i] = string.char(string.byte('a')+math.random(4)-1)
    c[i] = string.char(string.byte('a')+math.random(4)-1)
    d[i] = tostring(math.random(MAX))
    data[i] = tostring(MAX-i+1)
  end
  return a,b,c,d,data
end

local function get(a,b,c,d,table_name, row)
  --printf("get(%s, %s)=", table_name, row)
  local tmp
  row = tonumber(row)
  if     table_name == 'a' then tmp=a[row]
  elseif table_name == 'b' then tmp=b[row]
  elseif table_name == 'c' then tmp=c[row]
  elseif table_name == 'd' then tmp=d[row]
  end
  --print(tmp)
  return tmp
end

local function get_db(table_name, row)
--  printf("get_db(%s; %s)\n", table_name, row)
  local tmp
  if sharding == false then
    tmp = box.space[table_name]:select(tostring(row))
  else
    tmp = shard[table_name]:select(tostring(row))
  end
  if sharding == true then
--    print(table_name.."["..tostring(row).."]".." -> "..tmp[1][1][2])
    return tmp[1][1][2]
  else 
    return tmp[1][2]
  end
end

local layer = {}

local function execute_wide(expr, a,b,c,d,data)
  local results = {}
  local layer_num = 1
  local lnum = #(expr[1])
  local params = {}
  for n = 1,lnum,1 do --n = layer num
    for i = 1,#expr,1 do --i = expr num
      if results[i] == nil then results[i] = {'a'} end
      local k = expr[i][n]
      layer[i] = k
      params[i] = {layer, results, i, n}
    end
    local f = function (param)
      local layer, results, expr_i, layer_n
      layer = param[1]
      results = param[2]
      expr_i = param[3]
      layer_n = param[4]
      local tname = nil
      if (results[expr_i] ~= nil) then
        tname = results[expr_i][layer_n]
      else 
        return
      end 
      --final result means a number in the results, finish then
      if (tname == nil) or (string.find("abcdata", tname) == nil) then return end
      if tname == 'd' then
        local val
        if inmem then
          val = tonumber(d[tonumber(layer[expr_i])])
          results[expr_i][layer_n+1] = data[val]
        else
          val = tonumber(get_db('d', layer[expr_i]))
          results[expr_i][layer_n+1] = get_db('data', val)
        end
        Count = Count + 2
        return
      end
      if inmem == false then
        results[expr_i][layer_n+1] = get_db(tname, layer[expr_i])
      else
        results[expr_i][layer_n+1] = get(a,b,c,d, tname, layer[expr_i])
      end
      Count = Count + 1
    end
    execute_and_wait(params, f)
  end
  
  if debug then printf("results# = %d\n", #results) end
  return results
end

local function execExpr(i, e, results, a,b,c,d,data)
  local res = {}
  local tname = 'a'
  for s=1,#e,1 do
    if tname == 'd' then
      local val
      if inmem then
        val = tonumber(d[tonumber(e[s])])
        res[s] = data[val]
      else
        val = tonumber(get_db('d', e[s]))
        res[s] = get_db('data', val)
      end
      Count = Count + 2
      break
    end 
    if inmem then
      tname = get(a,b,c,d, tname, e[s])
    else
      tname = get_db(tname, e[s]) 
    end
    Count = Count + 1
    res[s] = tname
  end
  results[i] = res
end

exprCount = 0

local function execute(expr,a,b,c,d,data)
  local results = {}
  if debug then printf("execute(): expr# = %d\n", #expr) end
  local fiber = nil
  if deep == true then 
    fiber = require('fiber') 
    exprCount = #expr
  end
  for i=1,#expr,1 do
    local e = expr[i]
    if deep == false then
      execExpr(i, e, results, a,b,c,d,data)
    else
      fiber.create(function (i, e, results, a,b,c,d,data) 
          execExpr(i, e,results,a,b,c,d,data)
          exprCount = exprCount - 1 
        end, i, e, results, a,b,c,d,data)
    end
  end
  if deep == true then 
    while exprCount > 0 do
      fiber.sleep(1)
      print("exprCnt="..exprCount)    
    end
  end
  if debug then printf("results# = %d\n", #results) end
  return results
end

local function main()
  local t
  local t0=os.clock()
  local e 
  
  if debug then
    e = init_expressions_test()
  else
    e = init_expressions()
  end
  
  print("expressions:")
   
  --print
  if debug then
    for i = 1, #e, 1 do
      local z = e[i]
      print("expr #"..i)
      for j=1,#z,1 do
        print(z[j]..', ')
      end
    end 
  end
  
  t = os.clock()
  print("added "..#e.." expressions in "..1000*(t - t0).." ms.\nfilling data model. please wait.")

  t0 = os.clock()
  local a,b,c,d,data
  if inmem then
    if debug then
      a,b,c,d,data = init_inmem_test()
    else
      a,b,c,d,data = init_inmem()
    end
    t = os.clock()
    print("added "..(#a+#b+#c+#d+#data).." records in datamodel in "..1000*(t - t0).." ms")
    if debug then
      ptint("data model: ")
      for i = 1, #a, 1 do
        print(a[i]..', '..b[i]..', '..c[i]..', '..d[i], data[i])
      end
      print("end of data model")
    end
  else
    if sharding == false then
      init_db()
      t = os.clock()
      print("filled datamodel in "..1000*(t - t0).." ms (no shard, no batch)")
    else
      --print("shard is enabled, db was already initialized (on shard init using init_db callback)")
      local shrd = shard
      --queueing? operates in batch
      if (batch == true) then shrd = shard.q_begin() end
      local cnt=0
      for i = 1, MAX, 1 do
        if (batch == true) then
          shrd['a']:q_insert(cnt,{tostring(i), string.char(string.byte('a')+math.random(4)-1)})
          cnt=cnt+1
          shrd['b']:q_insert(cnt,{tostring(i), string.char(string.byte('a')+math.random(4)-1)})
          cnt=cnt+1
          shrd['c']:q_insert(cnt,{tostring(i), string.char(string.byte('a')+math.random(4)-1)})
          cnt=cnt+1
          shrd['d']:q_insert(cnt,{tostring(i), tostring(math.random(MAX))})
          cnt=cnt+1
          shrd['data']:q_insert(cnt,{tostring(i), tostring(MAX-i+1)})
          cnt=cnt+1
        else
          shrd['a']:insert{tostring(i), string.char(string.byte('a')+math.random(4)-1)}
          shrd['b']:insert{tostring(i), string.char(string.byte('a')+math.random(4)-1)}
          shrd['c']:insert{tostring(i), string.char(string.byte('a')+math.random(4)-1)}
          shrd['d']:insert{tostring(i), tostring(math.random(MAX))}
          shrd['data']:insert{tostring(i), tostring(MAX-i+1)}
        end
      end
      if (batch == true) then shrd:q_end() end
      print("filled datamodel in "..1000*(os.clock()-t0).." ms (shard=true, batch="..tostring(batch)..")")
    end
  end

  local res = {}
  if debug then REPEATS = 1 end

  print("processing test. please wait.")

  t0 = os.clock()
  for z=1,REPEATS,1 do
    if inmem then
      if wide == false then
        res[z] = execute(e,a,b,c,d,data)
      else 
        res[z] = execute_wide(e,a,b,c,d,data)
      end
    else
      if wide == false then
        res[z] = execute(e,nil,nil,nil,nil,nil)
      else
        res[z] = execute_wide(e,nil,nil,nil,nil,nil)
      end
    end
  end
  t = os.clock()
  
  if (debug) then
    print("repeats #="..#res)
    for i = 1,#res,1 do
      for j = 1,#res[i],1 do
        r = res[i][j]
        for k = 1,#r,1 do
          printf("%s, ", r[k])
        end
        printf("\n")
      end
    end
  end
  print("executed in "..1000.0*(t-t0).." ms; total dereferences: "..Count)
  print("executed in "..1000.0*(t-t0).." ms; single dereference time: "..((t-t0)*1000000)/Count.." \181s = "..((t-t0)*1000000000)/Count.." ns")
  if inmem == false then 
    os.exit()
  end
end

main()
