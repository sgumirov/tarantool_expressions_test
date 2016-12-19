shard = require('shard')
box = require('box')

--shard params: 
-- CONFIG THIS!
if binport == nil then
  binport = 33330
  shards_num = 3
end
--

cfg = {
  servers = {
--    { uri = 'localhost:33132', zone = '2' },
--    { uri = 'localhost:33133', zone = '3' },
--    { uri = 'localhost:33134', zone = '4' },
--    { uri = 'localhost:33135', zone = '5' },
--    { uri = 'localhost:33136', zone = '6' },
--    { uri = 'localhost:33137', zone = '7' },
  },
  redundancy = 1,
  login = 'demo',
  password = 'demo',
  binary = binport,
}
for i=1,shards_num,1 do
  cfg.servers[i]={ uri = 'localhost:'..tostring(33330+i-1), zone = tostring(i-1) }
end

box.cfg{
  log_level = 5;
  listen = cfg.binary;
  slab_alloc_arena = 0.1;
  wal_mode = 'none';
}

box.schema.user.create(cfg.login, { password = cfg.password })
box.schema.user.grant(cfg.login, 'read,write,execute', 'universe')

shard.check_shard = function(con)
  return con.space.data ~= nil
end
print("port="..cfg.binary)
shard.init(cfg)
shard.wait_connection()

print("Sharding is UP")
