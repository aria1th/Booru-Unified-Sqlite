from utils.gelboorutags import GelbooruTag as TagHandler
from db import load_db
from peewee import fn
from tqdm import tqdm
handler = TagHandler(exception_handle=0) #general for 0

def fix_tags(tags):
    result = handler.get_types(tags, verbose=True)[0]
    if result == "deprecated":
        return "general"
    return result

# test

db_dict = load_db('gelbooru2024-02.db')
GelbooruDB, GelbooruPost, GelbooruTag, GelbooruPostTagRelation = db_dict['db'], db_dict['Post'], db_dict['Tag'], db_dict['PostTagRelation']

# get tags which is not unknown type
tags = GelbooruTag.select().where(GelbooruTag.type != "unknown")
print(tags.count())
# get random post
tags = GelbooruTag.select().where(GelbooruTag.type == "unknown")
pbar = tqdm(tags, total=tags.count()) # Tag -> fix_tags(tag.name) -> [Tag.type]
fixed_count = 0
# batch update
batch_size = 1000
batch = []
for tag in tags:
    batch.append((fix_tags(tag.name), tag.id))
    if len(batch) >= batch_size:
        with GelbooruDB.atomic():
            pbar.set_description("Updating")
            for fixed_tag, tag_id in batch:
                GelbooruTag.update(type=fixed_tag).where(GelbooruTag.id == tag_id).execute()
        fixed_count += len(batch)
        batch = []
        pbar.update(batch_size)
        pbar.set_postfix(fixed=fixed_count)
    else:
        pbar.set_description("Collecting")
if batch:
    with GelbooruDB.atomic():
        for fixed_tag, tag_id in batch:
            GelbooruTag.update(type=fixed_tag).where(GelbooruTag.id == tag_id).execute()
    fixed_count += len(batch)
    pbar.set_postfix(fixed=fixed_count)
