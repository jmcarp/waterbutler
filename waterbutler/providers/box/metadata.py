import os

from waterbutler.core import metadata


class BaseBoxMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path):
        super().__init__(raw)
        self._path = path

    @property
    def provider(self):
        return 'box'


class BoxFolderMetadata(BaseBoxMetadata, metadata.BaseFolderMetadata):

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return os.path.join(str(self._path), self.name) + '/'


class BoxFileMetadata(BaseBoxMetadata, metadata.BaseFileMetadata):

    @property
    def name(self):
        return self.raw['name']

    @property
    def path(self):
        return os.path.join(str(self._path), self.name)

    @property
    def size(self):
        return self.raw.get('size')

    @property
    def modified(self):
        return self.raw.get('modified_at')

    @property
    def folder(self):
        return self.settings['folder']

    @property
    def content_type(self):
        return None


class BoxRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version(self):
        try:
            return self.raw['id']
        except KeyError:
            return self.raw['path'].split('/')[1]

    @property
    def version_identifier(self):
        return 'revision'

    @property
    def modified(self):
        try:
            return self.raw['modified_at']
        except KeyError:
            return self.raw.get('modified')

    # @property
    # def revision(self):
    #     return self.raw['etag']
