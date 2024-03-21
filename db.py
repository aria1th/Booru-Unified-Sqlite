from peewee import *
import tarfile
import jsonlines
import json
import sqlite3
import os

GELBOORU_KEYS_TO_DANBOORU = {
    "creator_id": "uploader_id",
    "width": "image_width",
    "has_children": "has_active_children",
    "file_url": "large_file_url",
    "preview_url": "file_url",
    "sample_url": "preview_file_url",
    "width": "image_width",
    "height": "image_height",
}

DANBOORU_KEYS_TO_GELBOORU = {value: key for key, value in GELBOORU_KEYS_TO_DANBOORU.items()}


def load_db(db_file: str):
    """
    Return a dictionary with the database objects.
    This allows multiple databases to be loaded in one program.
    """
    tag_cache_map = {}
    class BaseModel(Model):
        class Meta:
            database = SqliteDatabase(db_file)


    class EnumField(IntegerField):
        def __init__(self, enum_list, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.enum_list = enum_list
            self.enum_map = {value: index for index, value in enumerate(enum_list)}

        def db_value(self, value):
            if isinstance(value, str):
                return self.enum_map[value]
            assert isinstance(value, int)
            return value

        def python_value(self, value):
            if value is not None:
                return self.enum_list[value]


    # 'id', 'created_at', 'score', 'width', 'height', 'md5', 'directory', 'image', 'rating', 'source', 'change', 'owner', 'creator_id', 'parent_id', 'sample', 'preview_height', 'preview_width', 'tags', 'title', 'has_notes', 'has_comments', 'file_url', 'preview_url', 'sample_url', 'sample_height', 'sample_width', 'status', 'post_locked', 'has_children'

    class Post(BaseModel):
        # "id", "created_at", "uploader_id", "source", "md5", "parent_id", "has_children", "is_deleted", "is_banned", "pixiv_id", "has_active_children", "bit_flags", "has_large", "has_visible_children", "image_width", "image_height", "file_size", "file_ext", "rating", "score", "up_score", "down_score", "fav_count", "file_url", "large_file_url", "preview_file_url"
        id = IntegerField(primary_key=True)
        created_at = CharField()
        uploader_id = IntegerField() # creator_id in gelbooru
        source = CharField()
        md5 = CharField(null=True)
        parent_id = IntegerField(null=True)
        has_children = BooleanField()
        is_deleted = BooleanField(default=False, null=True)
        is_banned = BooleanField(default=False, null=True)
        pixiv_id = IntegerField(null=True)
        has_active_children = BooleanField(default=False, null=True) # has_children in gelbooru
        bit_flags = IntegerField(default=0, null=True)
        has_large = BooleanField(default=False, null=True)
        has_visible_children = BooleanField(default=False, null=True)

        image_width = IntegerField() # width in gelbooru
        image_height = IntegerField() # height in gelbooru
        file_size = IntegerField(default=0, null=True)
        file_ext = CharField(default="jpg", null=True)

        rating = EnumField(["general", "sensitive", "questionable", "explicit"])
        score = IntegerField()
        up_score = IntegerField(default=0, null=True)
        down_score = IntegerField(default=0, null=True)
        fav_count = IntegerField(default=0, null=True)

        file_url = CharField(null=True) # preview_url in gelbooru
        large_file_url = CharField(null=True) # sample_url in gelbooru
        preview_file_url = CharField(null=True) # file_url in gelbooru

        _tags: ManyToManyField = None # set by tags.bind
        _tags_cache = None

        @property
        def tag_count(self):
            return len(self.tag_list) if self.tag_list else 0

        @property
        def tag_count_general(self):
            return len(self.tag_list_general) if self.tag_list else 0

        @property
        def tag_count_artist(self):
            return len(self.tag_list_artist) if self.tag_list else 0

        @property
        def tag_count_character(self):
            return len(self.tag_list_character) if self.tag_list else 0

        @property
        def tag_count_copyright(self):
            return len(self.tag_list_copyright) if self.tag_list else 0

        @property
        def tag_count_meta(self):
            return len(self.tag_list_meta) if self.tag_list else 0

        @property
        def tag_list(self):
            if self._tags_cache is None:
                self._tags_cache = list(self._tags)
            return self._tags_cache

        @property
        def tag_list_general(self):
            return [tag for tag in self.tag_list if tag.type == "general"]

        @property
        def tag_list_artist(self):
            return [tag for tag in self.tag_list if tag.type == "artist"]

        @property
        def tag_list_character(self):
            return [tag for tag in self.tag_list if tag.type == "character"]

        @property
        def tag_list_copyright(self):
            return [tag for tag in self.tag_list if tag.type == "copyright"]

        @property
        def tag_list_meta(self):
            return [tag for tag in self.tag_list if tag.type == "meta"]

        @property
        def tag_list_unknown(self):
            return [tag for tag in self.tag_list if tag.type == "unknown"]


    class Tag(BaseModel):
        id = IntegerField(primary_key=True)
        name = CharField(unique=True)
        type = EnumField(["general", "artist", "character", "copyright", "meta", "unknown"]) # unknown is for gelbooru unbased tags, should be fixed in future
        popularity = IntegerField()
        _posts: ManyToManyField = None
        _posts_cache = None

        @property
        def posts(self):
            if self._posts_cache is None:
                self._posts_cache = list(self._posts)
            return self._posts_cache

        def __str__(self):
            return f"<Tag '{self.name}'>"

        def __repr__(self):
            return f"<Tag|#{self.id}|{self.name}|{self.type[:2]}>"

    def get_tag_by_id(tag_id):
        if tag_id not in tag_cache_map:
            tag_cache_map[tag_id] = Tag.get_by_id(tag_id)
        return tag_cache_map[tag_id]

    class PostTagRelation(BaseModel):
        post = ForeignKeyField(Post, backref="post_tags")
        tag = ForeignKeyField(Tag, backref="tag_posts")

    class LocalPost(BaseModel):
        id = IntegerField(primary_key=True)
        filepath = CharField(null=True)
        latentpath = CharField(null=True)
        post = ForeignKeyField(Post, backref="localpost")

        def __str__(self):
            return f"<LocalPost '{self.filepath}'>"

        def __repr__(self):
            return f"<LocalPost|#{self.id}|{self.filepath}|{self.latentpath}|{self.post}>"

    tags = ManyToManyField(Tag, backref="_posts", through_model=PostTagRelation)
    tags.bind(Post, "_tags", set_attribute=True)
    file_exists = os.path.exists(db_file)
    db = SqliteDatabase(db_file)
    Post._meta.database = db
    Tag._meta.database = db
    PostTagRelation._meta.database = db
    LocalPost._meta.database = db
    db.connect()
    print("Database connected.")
    if not file_exists:
        db.create_tables([Post, Tag, PostTagRelation])
        db.create_tables([LocalPost])
        db.commit()
        print("Database initialized.")
    assert db is not None, "Database is not loaded"
    return {
        "Post": Post,
        "Tag": Tag,
        "PostTagRelation": PostTagRelation,
        "LocalPost": LocalPost,
        "tags": tags,
        "get_tag_by_id": get_tag_by_id,
        "db": db,
    }

if __name__ == "__main__":
    test_tarfile = r'G:\gelboorupost\0M.tar.gz'
    # get first jsonl file from tarfile
    with tarfile.open(test_tarfile, 'r:gz') as tar:
        while True:
            member = tar.next()
            if member is None:
                break
            if member.isfile():
                if member.name.endswith('.jsonl'):
                    f = tar.extractfile(member)
                    json_data = jsonlines.Reader(f)
                    for line in json_data:
                        # get keys
                        print(line.keys())
                        print(line.values())
                        break
                    break
            else:
                print('not a file')
