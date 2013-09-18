import sys;
from config import __config;
# For MongoDB
from pymongo import MongoClient;

# For Fuse
import logging
from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time, sleep
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

# Others
import os.path
from threading import Thread,Condition, Event
import thread
import glob
import ntpath
#helper functions

def conf(key):
	return __config[key];
	
def setConf(key, val):
	__config[key] = val;
	
def get_parent_dir(path):
    return os.path.abspath(os.path.join(path, os.pardir))
    
def filename_from_path(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)
##################

#Init Mongo DB
client = MongoClient(conf('db_host'), conf('db_port'));
db = client[conf('db_name')];
############


path_sep = os.pathsep;


if not hasattr(__builtins__, 'bytes'):
    bytes = str

class DifSys(LoggingMixIn, Operations):
    locks = {};
    attr_write_lock = Condition();
    attrs = {};
    
    
    
    def __init__(self):
        self.fd = 0
        now = time()
        root = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2, path='/')
        if db.file.find({'path':'/'}).count() == 0:
            #db.file.insert(root);
            self.set_file_attr(root);
        thread.start_new_thread( self.update_attrs, ());
            
    ################################            
            
    def set_file_attr(self, attr):
        self.attrs[attr['path']] = attr;
        
    def get_file_attr(self, path):
        if path in self.attrs:
            return self.attrs[path];
        else:
            rset = db.file.find({'path':path});
            if rset.count() == 0:
                return None;
            else:
                return rset[0];
    def remove_file_attr(self, path):
        if path in self.attrs:
            del self.attrs[path];
        db.file.remove({'path':path});

    def update_attrs(self):
        while True:
            if self.attrs:
                self.attr_write_lock.acquire();
                for key, value in self.attrs.items():
                    db.file.update({'path':key}, value, True);
                    del self.attrs[key];
                self.attr_attr_write_locklock.release();
                
            
            sleep(conf('attr_update_interval'));


    def difsys_cmd(self, path):
        path = path.replace(conf('cmd_suffix'), '');
        if path.endswith('.piece_created'):
            path = path.replace('.piece_created', '');
            self.locks[path].set();
            del self.locks[path];
        return;

    #######
    contents = {}
    def get_piece_content(self, filepiecename):
        if filepiecename in self.contents:
            return self.contents[filepiecename];
        else:
            f = open(filename, 'rb');
            content = r.read();
            f.close();
            return content; 
    def set_piece_content(self, filepiecename, data):
        self.contents[filepiecename] = data;
    def flush_piece_content(self):
        if self.contents :
            for filepiecename, data in self.contents:
                f = open(filepiecename, 'wb');
                f.write(data);
                f.close();
    ################################  
    
    
    
    def chmod(self, path, mode):
        #attr = db.file.find({'path':path})[0];
        attr = self.get_file_attr(path);
        attr['st_mode'] &= 0770000
        attr['st_mode'] |= mode
        #db.file.update({'path':path}, attr );
        self.set_file_attr(attr);
        return 0

    def chown(self, path, uid, gid):
        attr = self.get_file_attr(path);
        attr['st_gid'] = gid;
        attr['st_uid'] = uid;
        self.set_file_attr(attr);
        #db.file.update({'path':path}, {'$set':{ 'st_gid': gid, 'st_uid': uid}} );

    def create(self, path, mode):
        if path.endswith(conf('cmd_suffix')):
            self.difsys_cmd(path);
            return 0
        else:
            attr = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                    st_size=0, st_ctime=time(), st_mtime=time(),
                                    st_atime=time(), path=path, parent=get_parent_dir(path))
            self.set_file_attr(attr);
            self.fd += 1
            return self.fd

    def getattr(self, path, fh=None):
        attr = self.get_file_attr(path);
        if attr == None:
            raise FuseOSError(ENOENT)

        return attr;

    def getxattr(self, path, name, position=0):
        xattrs = self.get_file_attr(path).get('xattrs', {});
        try:
            return xattrs[name];
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        attrs = self.get_file_attr(path).get('xattrs', {});
        return attrs.keys()

    def mkdir(self, path, mode):
        dir_attr = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), path=path, parent=get_parent_dir(path))
        #db.file.insert(dir_attr);
        self.set_file_attr(dir_attr);
        parent_dir = get_parent_dir(path);
        #db.file.update({'path':parent_dir}, { '$inc': { 'st_nlink': 1 } } );
        parent_attr = self.get_file_attr(parent_dir);
        parent_attr['st_nlink']+=1;
        self.set_file_attr(parent_attr);

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        file_attr = self.get_file_attr(path);
        
        #If the requested size exceed available data size
        if file_attr['st_size'] - offset < size:
            size = file_attr['st_size'] - offset;
            
        content = "";
        while size > 0 :
            file_offset = offset - (offset % conf('piece_length'));
            content_offset = offset - file_offset;
            filename = conf('fs_storage')+path+'.'+str(file_offset)+'.'+str(conf('piece_length'));
            while not os.path.exists(filename):
                self.locks[path] = Event();
                self.locks[path].wait();
                #self.locks[path] = Condition();
                #self.locks[path].acquire();
                #self.locks[path].wait();
                #self.locks[path].release();
            f = open(filename, 'rb');
            f.seek(content_offset);
            read_size = min(conf('piece_length')-content_offset, size);
            content = content + f.read(read_size);
            size = size - read_size;
            offset = offset + read_size;
            f.close();
        return content;

    def readdir(self, path, fh):
        resultset = db.file.find({'parent': path});
        results = ['.', '..'];
        for i in xrange(resultset.count()):
            needle = path;
            if(needle != '/'):
                needle = needle + '/';
            p = resultset[i]['path'].replace(needle, '');
            results.append(p);
        for key, val in self.attrs.items():
            if val['parent'] == path:
                needle = path;
                if(needle != '/'):
                    needle = needle + '/';
                p = val['path'].replace(needle, '');
                results.append(p);
        return list(set(results));

    def readlink(self, path):
        return;

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        return;

    def rmdir(self, path):
        empty = True;
        for key, val in self.attrs.items():
            if val['parent'] == path:
                empty = False;
                break;
        if db.file.find({'parent':path}).count() == 0 and empty:
            #db.file.remove({'path':path});
            self.remove_file_attr(path);
        else:
            raise FuseOSError(ENOTEMPTY);
        return;

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        #attrs = self.files[path].setdefault('attrs', {})
        #attrs[name] = value
        print path, name, value;
        attr = self.get_file_attr(path);
        xattrs = attr.setdefault('xattrs', {});
        xattrs[name] = value;
        self.set_file_attr(attr);

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        return

    def truncate(self, path, length, fh=None):
        return;

    def unlink(self, path):
        for fl in glob.glob(conf('fs_storage')+path+"*"):
            os.remove(fl);
        self.remove_file_attr(path);
        return;

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        attr = self.get_file_attr(path);
        attr['st_atime'] = atime;
        attr['st_mtime'] = mtime;
        self.set_file_attr(attr);
        #db.file.update({'path':path}, {'$set':{ 'st_atime': atime,  'st_mtime': mtime}} );

    def write(self, path, data, offset, fh):
        
        #create directory for storage
        file_dir = get_parent_dir(path);
        if not os.path.exists(conf('fs_storage')+file_dir):
            os.makedirs(conf('fs_storage')+file_dir);
            
        #Update file size
        file_attr = self.get_file_attr(path);
        if file_attr['st_size'] < offset + len(data):
            file_attr['st_size'] = offset + len(data);
            self.set_file_attr(file_attr);
            
        total_len = len(data);
        while len(data) > 0 :
            content_offset = offset % conf('piece_length');
            file_offset = offset - content_offset;
            space_length = conf('piece_length') - content_offset;
            if len(data) > space_length:
                content = data[:space_length];
            else:
                content = data;
            filename = conf('fs_storage')+path+'.'+str(file_offset)+'.'+str(conf('piece_length'));
            
            
            if content_offset > 0:
                fr = open(filename, 'r');
                old_content = fr.read();
                fr.close();
            
            fw = open(filename, 'wb');
            if content_offset > 0:
                fw.write(old_content[:content_offset]);
            fw.write(content);
            fw.close();
            
            data = data[len(content):];
            offset = offset + len(content);
        return total_len;




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG);
    if not os.path.exists(conf('fs_storage')):
        os.makedirs(conf('fs_storage'));
    if not os.path.exists(conf('fs_root')):
        os.makedirs(conf('fs_root'));
    fuse = FUSE(DifSys(), conf('fs_root'), foreground=True)
    
    
    
    
    
    
    
    
    
    
    
    
    
