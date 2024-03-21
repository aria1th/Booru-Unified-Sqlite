from db import load_db, GELBOORU_KEYS_TO_DANBOORU
import os
import datetime
import json
from tqdm import tqdm
def sanity_check(random_post):
    """
    Print out a random post and its informations
    """
    print(f"Post id : {random_post.id}")
    try:
        print(f"Post tags: {random_post.tag_list}, {len(random_post.tag_list)} tags, {random_post.tag_count} tags")
        print(f"Post general tags: {random_post.tag_list_general}, {len(random_post.tag_list_general)} tags, {random_post.tag_count_general} tags")
        print(f"Post artist tags: {random_post.tag_list_artist}, {len(random_post.tag_list_artist)} tags, {random_post.tag_count_artist} tags")
        print(f"Post character tags: {random_post.tag_list_character}, {len(random_post.tag_list_character)} tags, {random_post.tag_count_character} tags")
        print(f"Post copyright tags: {random_post.tag_list_copyright}, {len(random_post.tag_list_copyright)} tags, {random_post.tag_count_copyright} tags")
        print(f"Post meta tags: {random_post.tag_list_meta}, {len(random_post.tag_list_meta)} tags, {random_post.tag_count_meta} tags")
    except:
        pass
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
external_keys_to_find = open("external_keys.txt", 'r').read().split("\n")
print(len(external_keys_to_find))
dict_1 = load_db(r"G:\gelboorupost\danbooru2023.db")
dict_2 = load_db(r"G:\gelboorupost\gelbooru2024-02-simple.db")

DanbooruPost, DanbooruTag, DanbooruPostTagRelation = dict_1["Post"], dict_1["Tag"], dict_1["PostTagRelation"]

GelbooruPost, GelbooruTag, GelbooruPostTagRelation = dict_2["Post"], dict_2["Tag"], dict_2["PostTagRelation"]

def find_matching():
    selector = DanbooruPost.select().where(DanbooruPost.source.is_null(False) & (DanbooruPost.large_file_url.is_null(True)
                                                                                )).order_by(DanbooruPost.id.desc()).where(DanbooruPost.id < 6000000)

    # find if PostTagRelation has any of the external keys -> if so, find the post and print it
    selector = selector.join(DanbooruPostTagRelation).join(DanbooruTag).where(DanbooruTag.name << external_keys_to_find)

    # search for the post in gelbooru
    # if "width" and "height" is identical, we can assume that the post is the same
    # get list of posts with the same width and height + maybe created_at "date" range in 1-2 days

    # created_at is 2024-03-18T23:57:33.245-04:00 like string
    # gelbooru created_at is saved as Fri Jan 20 12:19:14 -0600 2023
    # we need to convert the string to a datetime object

    def convert_to_day(date, offset): # to 2024-03-18
        # reads date string, adds offset days and returns the date in the format "2024-03-18"
        return (datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f%z") + datetime.timedelta(days=offset)).strftime("%Y-%m-%d")

    def convert_to_day_gelbooru(date):
        return datetime.datetime.strptime(date, "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d")
    found_dict = {}
    pbar = tqdm(selector, total=selector.count())
    total_found = 0
    for result in pbar:
        
        finder = GelbooruPost.select(GelbooruPost.id).where(GelbooruPost.image_width == result.image_width, GelbooruPost.image_height == result.image_height).where(
            (GelbooruPost.created_at > convert_to_day(result.created_at, -2)) & (GelbooruPost.created_at < convert_to_day(result.created_at, 2)))
        if finder.exists():
            found_dict[result.id] = []
            for found in finder:
                found_dict[result.id].append(found.id)
            total_found += 1
            pbar.set_postfix({"found": total_found})
    # save to file
    with open("found_dict.json", 'w') as f:
        f.write(json.dumps(found_dict))

if __name__ == "__main__":
    find_matching()
    #print(len(found_dict))
    #print(found_dict)