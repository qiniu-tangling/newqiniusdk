# -*- coding: utf-8 -*-

import os
import time

from qiniu_new import config
from qiniu_new.utils import urlsafe_base64_encode, crc32, file_crc32, _file_iter, _chunk_iter
from qiniu_new import http
from .upload_progress_recorder import UploadProgressRecorder


def put_data(
        up_token, key, data, params=None, mime_type='application/octet-stream', check_crc=False, progress_handler=None):
    """上传二进制流到七牛

    Args:
        up_token:         上传凭证
        key:              上传文件名
        data:             上传二进制流
        params:           自定义变量，规格参考 http://developer.qiniu.com/docs/v6/api/overview/up/response/vars.html#xvar
        mime_type:        上传数据的mimeType
        check_crc:        是否校验crc32
        progress_handler: 上传进度

    Returns:
        一个dict变量，类似 {"hash": "<Hash string>", "key": "<Key string>"}
        一个ResponseInfo对象
    """
    crc = crc32(data) if check_crc else None
    return _form_put(up_token, key, data, params, mime_type, crc, progress_handler)


def clear_file_resume_history(key):
    """remove the resume history of the files
    Args:
        key: upload file name
    """
    recorder = UploadProgressRecorder()
    try:
        recorder.delete_upload_record(key)
    except OSError:
        pass

def put_file_with_chunk(up_token, key, file_path, params=None,
             mime_type='application/octet-stream', check_crc=False,
             progress_handler=None, upload_progress_recorder=None):
    ret = {}
    size = os.stat(file_path).st_size
    with open(file_path, 'rb') as input_stream:
        if size > config._BLOCK_SIZE * 2:
            ret, info = put_stream_with_chunk(up_token, key, input_stream, size, params,
                                   mime_type, progress_handler,
                                   upload_progress_recorder=upload_progress_recorder,
                                   modify_time=(int)(os.path.getmtime(file_path)))
        else:
            crc = file_crc32(file_path) if check_crc else None
            ret, info = _form_put(up_token, key, input_stream, params, mime_type, crc, progress_handler)
    return ret, info
    

def put_file(up_token, key, file_path, params=None,
             mime_type='application/octet-stream', check_crc=False,
             progress_handler=None, upload_progress_recorder=None):
    """上传文件到七牛

    Args:
        up_token:         上传凭证
        key:              上传文件名
        file_path:        上传文件的路径
        params:           自定义变量，规格参考 http://developer.qiniu.com/docs/v6/api/overview/up/response/vars.html#xvar
        mime_type:        上传数据的mimeType
        check_crc:        是否校验crc32
        progress_handler: 上传进度
        upload_progress_recorder: 记录上传进度，用于断点续传

    Returns:
        一个dict变量，类似 {"hash": "<Hash string>", "key": "<Key string>"}
        一个ResponseInfo对象
    """
    ret = {}
    size = os.stat(file_path).st_size
    with open(file_path, 'rb') as input_stream:
        if size > config._BLOCK_SIZE * 2:
            ret, info = put_stream(up_token, key, input_stream, size, params,
                                   mime_type, progress_handler,
                                   upload_progress_recorder=upload_progress_recorder,
                                   modify_time=(int)(os.path.getmtime(file_path)))
        else:
            crc = file_crc32(file_path) if check_crc else None
            ret, info = _form_put(up_token, key, input_stream, params, mime_type, crc, progress_handler)
    return ret, info

def put_file_with_threadpool(up_token, key, file_path, params=None,
             mime_type='application/octet-stream', check_crc=False,
             progress_handler=None, upload_progress_recorder=None, threads_count=config._DEFAULT_THREADS):
    """上传文件到七牛

    Args:
        up_token:         上传凭证
        key:              上传文件名
        file_path:        上传文件的路径
        params:           自定义变量，规格参考 http://developer.qiniu.com/docs/v6/api/overview/up/response/vars.html#xvar
        mime_type:        上传数据的mimeType
        check_crc:        是否校验crc32
        progress_handler: 上传进度
        upload_progress_recorder: 记录上传进度，用于断点续传
        threads_count: the default threads num.

    Returns:
        一个dict变量，类似 {"hash": "<Hash string>", "key": "<Key string>"}
        一个ResponseInfo对象
    """
    pass



def _form_put(up_token, key, data, params, mime_type, crc, progress_handler=None):
    fields = {}
    if params:
        for k, v in params.items():
            fields[k] = str(v)
    if crc:
        fields['crc32'] = crc
    if key is not None:
        fields['key'] = key
    fields['token'] = up_token
    url = 'http://' + config.get_default('default_up_host') + '/'
    name = key if key else 'filename'

    r, info = http._post_file(url, data=fields, files={'file': (name, data, mime_type)})
    if r is None and info.need_retry():
        if info.connect_failed:
            url = 'http://' + config.get_default('default_up_host_backup') + '/'
        if hasattr(data, 'read') is False:
            pass
        elif hasattr(data, 'seek') and (not hasattr(data, 'seekable') or data.seekable()):
            data.seek(0)
        else:
            return r, info
        r, info = http._post_file(url, data=fields, files={'file': (name, data, mime_type)})

    return r, info

def put_stream_with_chunk(up_token, key, input_stream, data_size, params=None,
               mime_type=None, progress_handler=None,
               upload_progress_recorder=None, modify_time=None):
    task = _Resume_with_chunk(up_token, key, input_stream, data_size, params, mime_type,
                   progress_handler, upload_progress_recorder, modify_time)
    return task.upload()

def put_stream_with_threadpool(up_token, key, input_stream, data_size, params=None,
               mime_type=None, progress_handler=None,
               upload_progress_recorder=None, modify_time=None, threads_count=config._DEFAULT_THREADS):
    task = _Resume_with_chunk(up_token, key, input_stream, data_size, params, mime_type,
                   progress_handler, upload_progress_recorder, modify_time)
    return task.upload()




class _Resume_with_chunk(object):
    """断点续上传类

    该类主要实现了分块上传，断点续上，以及相应地创建块和创建文件过程，详细规格参考：
    http://developer.qiniu.com/docs/v6/api/reference/up/mkblk.html
    http://developer.qiniu.com/docs/v6/api/reference/up/mkfile.html

    Attributes:
        up_token:         上传凭证
        key:              上传文件名
        input_stream:     上传二进制流
        data_size:        上传流大小
        params:           自定义变量，规格参考 http://developer.qiniu.com/docs/v6/api/overview/up/response/vars.html#xvar
        mime_type:        上传数据的mimeType
        progress_handler: 上传进度
        upload_progress_recorder:  记录上传进度，用于断点续传
        modify_time:      上传文件修改日期
    """

    def __init__(self, up_token, key, input_stream, data_size, params, mime_type,
                 progress_handler, upload_progress_recorder, modify_time):
        """初始化断点续上传"""
        self.up_token = up_token
        self.key = key
        self.input_stream = input_stream
        self.size = data_size
        self.params = params
        self.mime_type = mime_type
        self.progress_handler = progress_handler
        self.upload_progress_recorder = upload_progress_recorder or UploadProgressRecorder()
        self.modify_time = modify_time or time.time()
        self.chunk_offset = 0
        self.block_offset = 0
        self.chunkStatus = []
        s

    def record_upload_progress_with_chunk(self):
        record_data = {
            'size': self.size,
            'offset': self.block_offset,
            'contexts': [block['ctx'] for block in self.blockStatus],
            'chunk_host': self.chunkStatus[-1]['host'],
            'chunk_offset': self.chunk_offset,
            'chunk_context': self.chunkStatus[-1]['ctx']#only record the last finished chunk context
        }
        if self.modify_time:
            record_data['modify_time'] = self.modify_time

        print 'block offset:{0} chunk offset:{1}'.format(record_data['offset'],record_data['chunk_offset'])
        self.upload_progress_recorder.set_upload_record(self.key, record_data)

    def recovery_from_record_with_chunk(self):
        record = self.upload_progress_recorder.get_upload_record(self.key)
        if not record:
            return 0

        try:
            if not record['modify_time'] or record['size'] != self.size or \
                    record['modify_time'] != self.modify_time:
                return 0
        except KeyError:
            return 0

        self.blockStatus = [{'ctx': ctx} for ctx in record['contexts']]
        self.chunk_offset = record['chunk_offset']
        self.block_offset = record['offset']
        self.chunkStatus = [{'ctx': record['chunk_context'], 'host':record['chunk_host']}] if record['chunk_offset'] else []
        print 'block_offset is {0}; chunk offset is {1}'.format(record['offset'],record['chunk_offset'])
        return record['offset']

    def upload(self):
        """上传操作"""
        self.blockStatus = []
        host = config.get_default('default_up_host')
        self.block_offset = self.recovery_from_record_with_chunk()

        for block in _file_iter(self.input_stream, config._BLOCK_SIZE, self.block_offset):
            length = len(block)
            crc = crc32(block)
            ret, info = self.make_block_with_chunk(block, length, host, self.chunk_offset)

            if ret is None and not info.need_retry():
                return ret, info
            if info.connect_failed():
                host = config.get_default('default_up_host_backup')
            if info.need_retry() or length != ret['offset']:
                ret, info = self.make_block_with_chunk(block, length, host, self.chunk_offset)
            if ret is None or length != ret['offset']:
                return ret, info

            self.blockStatus.append(ret)
            self.block_offset += length
            self.chunk_offset = 0#complete  1 block, refresh the chunk offset to 0
            self.record_upload_progress_with_chunk()
            if(callable(self.progress_handler)):
                self.progress_handler(((len(self.blockStatus) - 1) * config._BLOCK_SIZE)+length, self.size)
        return self.make_file(host)

    def upload_with_threadpool(self):


        self.blockStatus = []
        host = config.get_default('default_up_host')
        self.block_offset = self.recovery_from_record_with_chunk()

        for block in _file_iter(self.input_stream, config._BLOCK_SIZE, self.block_offset):
            length = len(block)
            crc = crc32(block)
            ret, info = self.make_block_with_chunk(block, length, host, self.chunk_offset)

            if ret is None and not info.need_retry():
                return ret, info
            if info.connect_failed():
                host = config.get_default('default_up_host_backup')
            if info.need_retry() or length != ret['offset']:
                ret, info = self.make_block_with_chunk(block, length, host, self.chunk_offset)
            if ret is None or length != ret['offset']:
                return ret, info

            self.blockStatus.append(ret)
            self.block_offset += length
            self.chunk_offset = 0#complete  1 block, refresh the chunk offset to 0
            self.record_upload_progress_with_chunk()
            if(callable(self.progress_handler)):
                self.progress_handler(((len(self.blockStatus) - 1) * config._BLOCK_SIZE)+length, self.size)
        return self.make_file(host)



    def make_block_with_chunk(self, block, block_size, host, chunk_offset=0, chunkstatus=None):
        """创建块"""
#if chunk exists, upload the rest chunks 1st
        if chunk_offset==0:#upload the entire block
            url = self.block_url(host, block_size)
            ret, info =  self.post(url, block[0:config._CHUNK_SIZE])
            if block_size <= config._CHUNK_SIZE:
                return ret, info
            _ctx = ret['ctx']
            _host = ret['host']
            _offset = ret['offset']
            self.chunkStatus.append(ret)
            self.chunk_offset  = _offset
            self.record_upload_progress_with_chunk()            
        else:#resume from the record
            _offset = chunk_offset
            _ctx = chunkstatus
            _host = host


        for _chunk in _chunk_iter(block, config._CHUNK_SIZE, _offset):
            length = len(_chunk)
            crc = crc32(_chunk)
            url = self.chunk_url(_host, _ctx, _offset)
            ret, info = self.post(url, _chunk)

            if ret is None and not info.need_retry():
                        return ret, info
            if info.need_retry() or crc != ret['crc32']:
                        ret, info = self.post(url, _chunk)
                        if ret is None or crc != ret['crc32']:
                            return ret, info

            _ctx = ret['ctx']
            _host = ret['host']
            _offset = ret['offset']
            self.chunkStatus.append(ret)
            self.chunk_offset = _offset
            self.record_upload_progress_with_chunk()

        return ret, info

    def chunk_url(self, host, ctx, offset):
        return '{0}/bput/{1}/{2}'.format(host, ctx, offset)

    def block_url(self, host, size):
        return 'http://{0}/mkblk/{1}'.format(host, size)

    def file_url(self, host):
        url = ['http://{0}/mkfile/{1}'.format(host, self.size)]

        if self.mime_type:
            url.append('mimeType/{0}'.format(urlsafe_base64_encode(self.mime_type)))

        if self.key is not None:
            url.append('key/{0}'.format(urlsafe_base64_encode(self.key)))

        if self.params:
            for k, v in self.params.items():
                url.append('{0}/{1}'.format(k, urlsafe_base64_encode(v)))

        url = '/'.join(url)
        return url

    def make_file(self, host):
        """创建文件"""
        url = self.file_url(host)
        body = ','.join([status['ctx'] for status in self.blockStatus])
        return self.post(url, body)

    def post(self, url, data):
        return http._post_with_token(url, data, self.up_token)


def put_stream(up_token, key, input_stream, data_size, params=None,
               mime_type=None, progress_handler=None,
               upload_progress_recorder=None, modify_time=None):
    task = _Resume(up_token, key, input_stream, data_size, params, mime_type,
                   progress_handler, upload_progress_recorder, modify_time)
    return task.upload()


class _Resume(object):
    """断点续上传类

    该类主要实现了分块上传，断点续上，以及相应地创建块和创建文件过程，详细规格参考：
    http://developer.qiniu.com/docs/v6/api/reference/up/mkblk.html
    http://developer.qiniu.com/docs/v6/api/reference/up/mkfile.html

    Attributes:
        up_token:         上传凭证
        key:              上传文件名
        input_stream:     上传二进制流
        data_size:        上传流大小
        params:           自定义变量，规格参考 http://developer.qiniu.com/docs/v6/api/overview/up/response/vars.html#xvar
        mime_type:        上传数据的mimeType
        progress_handler: 上传进度
        upload_progress_recorder:  记录上传进度，用于断点续传
        modify_time:      上传文件修改日期
    """

    def __init__(self, up_token, key, input_stream, data_size, params, mime_type,
                 progress_handler, upload_progress_recorder, modify_time):
        """初始化断点续上传"""
        self.up_token = up_token
        self.key = key
        self.input_stream = input_stream
        self.size = data_size
        self.params = params
        self.mime_type = mime_type
        self.progress_handler = progress_handler
        self.upload_progress_recorder = upload_progress_recorder or UploadProgressRecorder()
        self.modify_time = modify_time or time.time()

        print(self.modify_time)
        print(modify_time)

    def record_upload_progress(self, offset):
        record_data = {
            'size': self.size,
            'offset': offset,
            'contexts': [block['ctx'] for block in self.blockStatus]
        }
        if self.modify_time:
            record_data['modify_time'] = self.modify_time

        print(record_data)
        self.upload_progress_recorder.set_upload_record(self.key, record_data)

    def recovery_from_record(self):
        record = self.upload_progress_recorder.get_upload_record(self.key)
        if not record:
            return 0

        try:
            if not record['modify_time'] or record['size'] != self.size or \
                    record['modify_time'] != self.modify_time:
                return 0
        except KeyError:
            return 0

        self.blockStatus = [{'ctx': ctx} for ctx in record['contexts']]
        print 'offset is {0}'.format(record['offset'])
        return record['offset']

    def upload(self):
        """上传操作"""
        self.blockStatus = []
        host = config.get_default('default_up_host')
        offset = self.recovery_from_record()
        for block in _file_iter(self.input_stream, config._BLOCK_SIZE, offset):
            length = len(block)
            crc = crc32(block)
            ret, info = self.make_block(block, length, host)
            if ret is None and not info.need_retry():
                return ret, info
            if info.connect_failed():
                host = config.get_default('default_up_host_backup')
            if info.need_retry() or crc != ret['crc32']:
                ret, info = self.make_block(block, length, host)
                if ret is None or crc != ret['crc32']:
                    return ret, info

            self.blockStatus.append(ret)
            offset += length
            self.record_upload_progress(offset)
            if(callable(self.progress_handler)):
                self.progress_handler(((len(self.blockStatus) - 1) * config._BLOCK_SIZE)+length, self.size)
        return self.make_file(host)

    def make_block(self, block, block_size, host):
        """创建块"""
        url = self.block_url(host, block_size)
        return self.post(url, block)

    def block_url(self, host, size):
        return 'http://{0}/mkblk/{1}'.format(host, size)

    def file_url(self, host):
        url = ['http://{0}/mkfile/{1}'.format(host, self.size)]

        if self.mime_type:
            url.append('mimeType/{0}'.format(urlsafe_base64_encode(self.mime_type)))

        if self.key is not None:
            url.append('key/{0}'.format(urlsafe_base64_encode(self.key)))

        if self.params:
            for k, v in self.params.items():
                url.append('{0}/{1}'.format(k, urlsafe_base64_encode(v)))

        url = '/'.join(url)
        return url

    def make_file(self, host):
        """创建文件"""
        url = self.file_url(host)
        body = ','.join([status['ctx'] for status in self.blockStatus])
        return self.post(url, body)

    def post(self, url, data):
        return http._post_with_token(url, data, self.up_token)
