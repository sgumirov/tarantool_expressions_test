
MAX = 1000000
DEPTH=4
WIDTH=2500
Count=0
debug=false

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

local function init_inmem()
  local a={}
  local b={}
  local c={}
  local d={}
  for i = 1, MAX, 1 do
    a[i] = string.char(string.byte('a')+math.random(4)-1)
    b[i] = string.char(string.byte('a')+math.random(4)-1)
    c[i] = string.char(string.byte('a')+math.random(4)-1)
    d[i] = tostring(i)
  end
  return a,b,c,d
end

local function execute(expr,a,b,c,d)
  local results = {}
  for i=1,#expr,1 do
    local e = expr[i]
    local table='a'
    --print('i='..i..", table="..table)
    for s=1,#e,1 do
      local index = tonumber(e[s])
      Count = Count + 1
      local tmp
      if table == 'a' then tmp=a[index]
      elseif table == 'b' then tmp=b[index]
      elseif table == 'c' then tmp=c[index]
      elseif table == 'd' then tmp=d[index]
      end
      table = tmp
      if s==4 then 
        results[i] = index
      end
    end
  end
  return results
end

local function main()
  --init_db()
  
  local t
  local t0=os.clock()
  local e = init_expressions()
   
  --print
  if debug then
    for i = 1, #e, 1 do
      local z = e[i]
      print(z)
      for j=1,#z,1 do
        print(z[j]..', ')
      end
    end 
  end
  
  t = os.clock()
  print("added "..#e.." expressions in "..1000*(t - t0).." ms")

  t0 = os.clock()
  local a,b,c,d = init_inmem()
  t = os.clock()
  print("added "..(#a+#b+#c+#d).." records in datamodel in "..1000*(t - t0).." ms")

  if debug then
    for i = 1, #a, 1 do
      print(a[i]..', '..b[i]..', '..c[i]..', '..d[i])
    end
  end

  local res = {}
  t0 = os.clock()
  for z=1,10,1 do
    res[t] = execute(e,a,b,c,d)
  end
  t = os.clock()
  for i = 1,#res,1 do
    print(res[i])
    for j = 1,#res[i],1 do
      print(res[i][j])
    end
  end
  print("executed in "..1000.0*(t-t0).." ms; total dereferences: "..Count)
  print("executed in "..1000.0*(t-t0).." ms; total dereferences: "..Count)
end

--initialize and fill datamodel tables to use for dereferencing expressions
local function init_db()
  box.schema.space.create('a')
  box.schema.space.create('b')
  box.schema.space.create('c')
  box.schema.space.create('d')
  box.space.a:create_index('primary')
  for i = 1, MAX, 1 do
    box.space.a.insert{tostring(i), {'a'+math.random(3)}}
    box.space.b.insert{tostring(i), {'a'+math.random(3)}}
    box.space.c.insert{tostring(i), {'a'+math.random(3)}}
    box.space.d.insert{tostring(i), {'a'+math.random(3)}}
  end
end

main()
