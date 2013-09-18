__config = {};
__config['db_host'] = '192.168.56.101';
__config['db_port'] = 27017;
__config['db_name'] = 'difsys';
__config['fs_root'] = '/root/fs';
__config['fs_storage'] = '/root/_fs_storage';

__config['piece_length'] = 3*1024*1024; #50MB

__config['cmd_suffix'] = '.difsys_cmd';

#Interval in seconds between each attribute update to database
__config['attr_update_interval'] = 20;
