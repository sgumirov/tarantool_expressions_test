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
end
init_db()