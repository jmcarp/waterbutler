import os
import http
import json
import asyncio

from waterbutler.core import utils
from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions

from waterbutler.providers.box import settings
from waterbutler.providers.box.metadata import BoxRevision
from waterbutler.providers.box.metadata import BoxFileMetadata
from waterbutler.providers.box.metadata import BoxFolderMetadata


class BoxPath(utils.WaterButlerPath):

    def __init__(self, folder, path, prefix=True, suffix=False):
        super().__init__(path, prefix=prefix, suffix=suffix)
        self._folder = folder
        full_path = os.path.join(folder, path.lstrip('/'))
        self._full_path = self._format_path(full_path)

    @property
    def parent(self):
        cls = self.__class__
        return cls(self._folder, '/'.join(self._parts[:-1]) + '/', prefix=self._prefix, suffix=self._suffix)

    @property
    def child(self):
        cls = self.__class__
        path = '/' + '/'.join(self._parts[2:])
        if self.is_dir:
            path += '/'
        path = path.replace('//', '/')
        return cls(self._folder, path, prefix=self._prefix, suffix=self._suffix)


class BoxProvider(provider.BaseProvider):

    BASE_URL = settings.BASE_URL

    def __init__(self, auth, credentials, settings):
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']
        self.path = self.settings['path']

    @property
    def default_headers(self):
        return {
            'Authorization': 'Bearer {}'.format(self.token),
        }

    @asyncio.coroutine
    def download(self, path, revision=None, **kwargs):
        data = yield from self.metadata(path, raw=True)
        query = {}
        if revision and revision != data['id']:
            query['version'] = revision

        resp = yield from self.make_request(
            'GET',
            self.build_url('files', data['id'], 'content', **query),
            expects=(200, ),
            throws=exceptions.DownloadError,
        )
        return streams.ResponseStreamReader(resp)

    @asyncio.coroutine
    def upload(self, stream, path, **kwargs):
        path = BoxPath(self.path, path)
        try:
            metadata = yield from self.metadata(str(path), raw=True)
        except exceptions.MetadataError:
            created = True
            if path.parent.is_root:
                folder_id = self.folder
            else:
                parent_metadata = yield from self.metadata(str(path.parent).rstrip('/'), raw=True)
                folder_id = parent_metadata['id']
            data = yield from self._upload(stream, path.name, folder_id)
        else:
            created = False
            data = yield from self._upload(stream, path.name, metadata['parent']['id'], file_id=metadata['id'])

        return BoxFileMetadata(data['entries'][0], path.parent).serialized(), created

    @asyncio.coroutine
    def delete(self, path, **kwargs):
        #'etag' of the file can be included as an ‘If-Match’
        #header to prevent race conditions
        data = yield from self.metadata(path, raw=True)
        yield from self.make_request(
            'DELETE',
            self.build_url('files', data['id']),
            expects=(204, ),
            throws=exceptions.DeleteError,
        )

    @asyncio.coroutine
    def metadata(self, path, folder_id=None, original_path=None, raw=False, **kwargs):
        path = BoxPath(self.path, path)
        original_path = original_path or path
        folder_id = folder_id or self.folder
        child = path.child

        if not path.is_leaf:
            data = yield from self._get_folder_meta(folder_id, title=path.parts[1], path=original_path, raw=True)
            return (yield from self.metadata(str(child), folder_id=data['id'], original_path=original_path, raw=raw))

        if path.is_file:
            data = yield from self._get_folder_meta(folder_id, title=path.parts[1], path=original_path, raw=True)
            if data['type'] == 'folder':
                return data
            return (yield from self._get_file_meta(data['id'], original_path, raw=raw))

        return (yield from self._get_folder_meta(folder_id, original_path, raw=raw))

    @asyncio.coroutine
    def revisions(self, path, **kwargs):
        #from https://developers.box.com/docs/#files-view-versions-of-a-file :
        #Alert: Versions are only tracked for Box users with premium accounts.
        #Few users will have a premium account, return only current if not
        metadata = yield from self.metadata(path, raw=True)
        response = yield from self.make_request(
            'GET',
            self.build_url('files', metadata['id'], 'versions'),
            expects=(200, ),
            throws=exceptions.RevisionsError,
        )
        data = yield from response.json()

        ret = []
        curr = yield from self.metadata(path, raw=True)
        ret.append(BoxRevision(curr).serialized())

        for item in data['entries']:
            ret.append(BoxRevision(item).serialized())

        return ret

    @asyncio.coroutine
    def _get_file_meta(self, file_id, path, raw=False):
        resp = yield from self.make_request(
            'GET',
            self.build_url('files', file_id),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        data = yield from resp.json()
        if data:
            if raw:
                return data
            return BoxFileMetadata(data, path).serialized()

        raise exceptions.MetadataError('Unable to find file.', code=http.client.NOT_FOUND)

    @asyncio.coroutine
    def _get_folder_meta(self, folder_id, path, title=None, raw=False):
        resp = yield from self.make_request(
            'GET',
            self.build_url('folders', folder_id, 'items'),
            expects=(200, ),
            throws=exceptions.MetadataError,
        )

        data = yield from resp.json()

        entries = data['entries']
        if title:
            entries = [each for each in entries if each['name'] == title]
            if len(entries) != 1:
                raise exceptions.MetadataError(data, code=http.client.NOT_FOUND)
            return entries[0]

        if raw:
            return entries

        ret = []
        for item in data['entries']:
            if item['type'] == 'folder':
                ret.append(BoxFolderMetadata(item, path).serialized())
            else:
                ret.append(BoxFileMetadata(item, path).serialized())
        return ret

    def _upload(self, stream, name, folder_id, file_id=None):
        form_stream = streams.FormDataStream(
            attributes=json.dumps({
                'name': name,
                'parent': {'id': folder_id},
            }),
        )
        form_stream.add_file('file', stream, name, disposition='form-data')

        segments = ['files', 'content']
        if file_id:
            segments.insert(1, file_id)

        resp = yield from self.make_request(
            'POST',
            self._build_upload_url(*segments),
            data=form_stream,
            headers=form_stream.headers,
            expects=(201, ),
            throws=exceptions.UploadError,
        )
        data = yield from resp.json()

        return data

    def _build_upload_url(self, *segments, **query):
        return provider.build_url(settings.BASE_UPLOAD_URL, *segments, **query)
