# find any post that has 'source' = 'https://twitter.com/uni__520/status/1615678493668573185'

import jsonlines
import tarfile
from tqdm import tqdm
import datetime
def find_something():
    for i in range(9, -1, -1):
        jsonl_tarfile = f'G:\\gelboorupost\\{i}M.tar.gz'
        with tarfile.open(jsonl_tarfile, 'r:gz') as tar:
            for member in tqdm(tar):
                if member.isfile() and member.name.endswith('.jsonl'):
                    f = tar.extractfile(member)
                    json_data = jsonlines.Reader(f)
                    for data in json_data:
                        if data['width'] == 2056 and data['height'] == 3058:
                            print(data)
                            break
                    else:
                        continue
                    break
            else:
                continue
            break
from db import *
import json
import os
from tqdm import tqdm
from functools import cache
import tarfile

@cache
def get_or_create_tag(tag_name: str, tag_type: str, optional_tag_id: int = None):
    """
    Get a tag if it exists, otherwise create it.
    """
    tag = Tag.get_or_none(Tag.name == tag_name)
    if tag is None:
        if optional_tag_id is not None:
            assert isinstance(optional_tag_id, int), f"optional_tag_id must be an integer, not {type(optional_tag_id)}"
            tag = Tag.create(id=optional_tag_id, name=tag_name, type=tag_type, popularity=0)
        else:
            tag = Tag.create(name=tag_name, type=tag_type, popularity=0)
    return tag

def create_tag_or_use(tag_name: str, tag_type: str, optional_tag_id: int = None):
    """
    Create a tag if it does not exist, otherwise return the existing tag.
    This function also increments the popularity of the tag.
    """
    tag = get_or_create_tag(tag_name, tag_type, optional_tag_id)
    # we don't have to check tag type since its unique
    tag.popularity = 1
    return tag

def iterate_jsonl(file_path: str):
    """
    Iterate through a jsonl file and yield each line as a dictionary.
    """
    if isinstance(file_path, str) and os.path.exists(file_path):
        with open(file_path, "r") as f:
            for line in f:
                yield json.loads(line)
    elif hasattr(file_path, "read"):
        for line in file_path:
            yield json.loads(line)
    else:
        raise ValueError("file_path must be a string or a file-like object")
JSONL_RATING_CONVERSION = {
    "q": "questionable",
    "s": "sensitive",
    "e": "explicit",
    "g": "general",
    "safe" : "general",
}

def create_tags(tag_string:str, tag_type:str):
    """
    Create tags from a tag string.
    """
    for tag in tag_string.split(" "):
        if not tag or tag.isspace():
            continue
        tag = create_tag_or_use(tag, tag_type)
        yield tag
# 'id', 'created_at', 'score', 'width', 'height', 'md5', 'directory', 'image', 'rating', 'source', 'change', 'owner', 'creator_id', 'parent_id', 'sample', 'preview_height', 'preview_width', 'tags', 'title', 'has_notes', 'has_comments', 'file_url', 'preview_url', 'sample_url', 'sample_height', 'sample_width', 'status', 'post_locked', 'has_children'

def get_conversion_key(data, key: str):
    """
    Get the conversion key for a key.
    """
    access_key = DANBOORU_KEYS_TO_GELBOORU.get(key, key)
    if access_key == "rating":
        return JSONL_RATING_CONVERSION.get(data.get(access_key, None), data.get(access_key, None))
    elif access_key == "created_at": #  Fri Jan 20 12:19:14 -0600 2023 -> 2023-01-20T12:19:14-06:00 
        data_created_at = data.get(access_key, None)
        if data_created_at is not None:
            data_created_at = datetime.datetime.strptime(data_created_at, "%a %b %d %H:%M:%S %z %Y")
            return data_created_at.isoformat()
    return data.get(access_key, None)

def create_post(json_data, policy="ignore"):
    """
    Create a post from a json dictionary.
    Policy can be 'ignore' or 'replace'
        Note that file_url, large_file_url, and preview_file_url are optional.
    """
    assert "id" in json_data, "id is not in json_data"
    post_id = json_data["id"]
    tags = json_data.get("tags", "").split(" ")
    all_tags = []
    all_tags += [create_tags(json_data.get("tags", ""), "general")]
    
    if Post.get_or_none(Post.id == post_id) is not None:
        if policy == "ignore":
            print(f"Post {post_id} already exists")
            return
        elif policy == "replace":
            Post.delete_by_id(post_id)
        else:
            raise ValueError(f"Unknown policy {policy}, must be 'ignore' or 'replace'")
    post = Post.create(
        **{key: get_conversion_key(json_data, key) for key in[
            "id", "created_at", "uploader_id", "source", "md5", "parent_id", "has_children", "is_deleted", "is_banned", "pixiv_id", "has_active_children", "bit_flags", "has_large", "has_visible_children", "image_width", "image_height", "file_size", "file_ext", "rating", "score", "up_score", "down_score", "fav_count"
        ]
        }
    )
    for tags in all_tags:
        for tag in tags:
            PostTagRelation.create(post=post, tag=tag)
    return post

def read_and_create_posts(file_path: str, policy="ignore"):
    """
    Read a jsonl file and create the posts in the database.
        Policy can be 'ignore' or 'replace'
    """
    for json_data in iterate_jsonl(file_path):
        create_post(json_data, policy)
        #print(f"Created post {json_data['id']}")

def create_db_from_folder(folder_path: str, policy="ignore"):
    """
    Create a database from a folder of jsonl files.
    This recursively searches the folder for jsonl files.
        Policy can be 'ignore' or 'replace'
    """
    global db
    assert db is not None, "Database is not loaded"
    all_jsonl_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".jsonl"):
                all_jsonl_files.append(os.path.join(root, file))
    with db.atomic():
        for file in tqdm(all_jsonl_files):
            read_and_create_posts(file, policy)

def create_db_from_tarfile(tarfile_path: str, policy="ignore", read_method="r:gz"):
    """
    Create a database from a tarfile of jsonl files.
        Policy can be 'ignore' or 'replace'
    """
    global db
    assert db is not None, "Database is not loaded"
    all_jsonl_files = []
    with tarfile.open(tarfile_path, read_method) as tar:
        for tarinfo in tar:
            if tarinfo.isfile() and tarinfo.name.endswith(".jsonl"):
                all_jsonl_files.append(tarinfo)
        with db.atomic():
            for tarinfo in tqdm(all_jsonl_files):
                with tar.extractfile(tarinfo) as f:
                    for json_data in iterate_jsonl(f):
                        create_post(json_data, policy)
                        #print(f"Created post {json_data['id']}")

def sanity_check(order="random"):
    """
    Print out a random post and its informations
    """
    if order == "random":
        random_post = Post.select().order_by(fn.Random()).limit(1).get()
    else:
        random_post = Post.select().limit(1).get()
    print(f"Post id : {random_post.id}")
    try:
        print(f"Post tags: {random_post.tag_list}, {len(random_post.tag_list)} tags, {random_post.tag_count} tags")
        print(f"Post general tags: {random_post.tag_list_general}, {len(random_post.tag_list_general)} tags, {random_post.tag_count_general} tags")
        print(f"Post artist tags: {random_post.tag_list_artist}, {len(random_post.tag_list_artist)} tags, {random_post.tag_count_artist} tags")
        print(f"Post character tags: {random_post.tag_list_character}, {len(random_post.tag_list_character)} tags, {random_post.tag_count_character} tags")
        print(f"Post copyright tags: {random_post.tag_list_copyright}, {len(random_post.tag_list_copyright)} tags, {random_post.tag_count_copyright} tags")
        print(f"Post meta tags: {random_post.tag_list_meta}, {len(random_post.tag_list_meta)} tags, {random_post.tag_count_meta} tags")
    except Exception as e:
        print(f"Error: {e}")
    print(f"Post rating: {random_post.rating}")
    print(f"Post score: {random_post.score}")
    print(f"Post fav_count: {random_post.fav_count}")
    print(f"Post source: {random_post.source}")
    print(f"Post created_at: {random_post.created_at}")
    print(f"Post file_url: {random_post.file_url}")
    print(f"Post large_file_url: {random_post.large_file_url}")
    print(f"Post preview_file_url: {random_post.preview_file_url}")
    print(f"Post image_width: {random_post.image_width}")
    print(f"Post image_height: {random_post.image_height}")
    print(f"Post file_size: {random_post.file_size}")
    print(f"Post file_ext: {random_post.file_ext}")
    print(f"Post uploader_id: {random_post.uploader_id}")
    print(f"Post pixiv_id: {random_post.pixiv_id}")
    print(f"Post has_children: {random_post.has_children}")
    print(f"Post is_deleted: {random_post.is_deleted}")
    print(f"Post is_banned: {random_post.is_banned}")
    print(f"Post has_active_children: {random_post.has_active_children}")
    print(f"Post has_large: {random_post.has_large}")
    print(f"Post has_visible_children: {random_post.has_visible_children}")
    print(f"Post bit_flags: {random_post.bit_flags}")

if __name__ == "__main__":
    db_dict = load_db("gelbooru2024-02-simple.db") # this is db without tag info
    Post, Tag, PostTagRelation = db_dict["Post"], db_dict["Tag"], db_dict["PostTagRelation"]
    db = db_dict["db"]
    #read_and_create_posts(r"C:\sqlite\0_99.jsonl")
    #create_db_from_folder(r'D:\danbooru-0319') # if you have a folder of jsonl files
    for i in range(0,10):
        create_db_from_tarfile(rf"G:\gelboorupost\{i}M.tar.gz")
