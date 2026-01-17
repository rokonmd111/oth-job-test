import os
import requests
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import time
import re
import copy 
from datetime import datetime, timedelta
import dateparser 
import json


# =========================================================
# কনফিগারেশন সেটিংস (Configuration Settings)
# =========================================================

TARGET_LISTING_URL = os.getenv('TARGET_URL')
BLOG_ID = os.getenv('BLOG_ID')

SCOPES = ['https://www.googleapis.com/auth/blogger']
MAX_POSTS_TO_LOAD = int(os.getenv('MAX_POSTS', 50))
POST_DELAY_SECONDS = 10 
DELETE_DELAY_SECONDS = 1
SCRAPED_POST_TAG = os.getenv('POST_TAG', 'অন্যান্য')

# 🎯 নতুন ডেট ট্যাগ প্যাটার্ন
WEB_END_DATE_TAG_PREFIX = 'WebEndDate:'

# =========================================================
# সহায়ক ফাংশন: API অনুমোদিত সার্ভিস অবজেক্ট তৈরি
# =========================================================

def get_blogger_service():
    """Google Blogger API-এর জন্য মেমোরি থেকে ক্রেডেনশিয়াল লোড করে।"""
    creds = None
    
    google_token_json = os.environ.get('GOOGLE_TOKEN')
    google_creds_json = os.environ.get('GOOGLE_CREDENTIALS')

    if google_token_json:
        token_info = json.loads(google_token_json)
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if google_creds_json:
                secret_info = json.loads(google_creds_json)
                flow = InstalledAppFlow.from_client_config(secret_info, SCOPES)
                creds = flow.run_local_server(port=0)

    return build('blogger', 'v3', credentials=creds)

# =========================================================
# 🔄 ধাপ ৩.১: বিদ্যমান পোস্ট টাইটেল সংগ্রহ (সকল)
# =========================================================

def get_existing_titles(service, blog_id):
    """ব্লগ থেকে SCRAPED_POST_TAG যুক্ত পোস্টের বর্তমান টাইটেলগুলির সেট সংগ্রহ করে।"""
    print("        🔍 ডুপ্লিকেশন চেক: ব্লগের সকল 'অন্যান্য' ট্যাগযুক্ত পোস্ট টাইটেল লোড করা হচ্ছে...")
    existing_titles = set()
    try:
        # শুধুমাত্র নির্দিষ্ট ট্যাগ যুক্ত পোস্টগুলি ফিল্টার করা
        response = service.posts().list(
            blogId=blog_id, 
            labels=SCRAPED_POST_TAG, 
            fetchBodies=False, 
            maxResults=500 
        ).execute()
        
        posts = response.get('items', [])
        for post in posts:
            existing_titles.add(post['title'])
            
        # 🌟 টার্মিনালে সর্বশেষ টাইটেল প্রিন্ট করা 
        latest_title = posts[0].get('title', 'কোনো পোস্ট নেই') if posts else 'কোনো পোস্ট নেই'
        print(f"        ✅ ব্লগে '{SCRAPED_POST_TAG}' ট্যাগযুক্ত বিদ্যমান পোস্ট পাওয়া গেছে: {len(existing_titles)} টি।")
        print(f"        ℹ️ আপনার ব্লগের সর্বশেষ পোস্টের টাইটেল (চেকের জন্য): **{latest_title}**")
        
    except Exception as e:
        print(f"        ❌ বিদ্যমান পোস্ট লোড করার সময় ত্রুটি: {e}")
        
    return existing_titles

# =========================================================
# 🚀 ধাপ ৩: পোস্টের তালিকা সংগ্রহ ও তারিখ ফিল্টারিং
# =========================================================

def get_all_post_links_and_details(listing_url):
    """আর্কাইভ পেজ থেকে পোস্টের URL, শিরোনাম, এবং ডেটলাইন সংগ্রহ করে এবং ফিল্টার করে।"""
    print(f"\n▶️ ধাপ ৩: পোস্টের তালিকা সংগ্রহ শুরু হচ্ছে: {listing_url}")
    today = datetime.now().date()
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(listing_url, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ পোস্ট তালিকা রিকোয়েস্ট ব্যর্থ হয়েছে: {e}")
        return []

    soup = BeautifulSoup(response.text, 'lxml')
    all_target_details = []

    # শুধুমাত্র পোস্ট লিঙ্ক টার্গেট করা
    all_links = soup.find_all('a', href=re.compile(r'/\d{4}/\d{2}/')) 

    for a_tag in all_links:
        post_url = a_tag.get('href')
        r_snippetized_div = a_tag.find('div', class_='r-snippetized')

        if r_snippetized_div:
            snippet_body_tag = r_snippetized_div.find('div', class_='snippet-body')
            deadline_text = snippet_body_tag.text.strip() if snippet_body_tag else ""

            temp_r_snippetized = copy.copy(r_snippetized_div)
            
            # snippet-body ডিকম্পোজ করা 
            if temp_r_snippetized.find('div', class_='snippet-body'):
                temp_r_snippetized.find('div', class_='snippet-body').decompose()
            
            title = temp_r_snippetized.text.strip()
            
            if 'blogspot.com/' in post_url and len(title) > 5:
                            
                is_deadline_post = re.search(r'deadline|সময়সীমা', deadline_text, re.IGNORECASE)
                is_result_post = 'চূড়ান্ত ফলাফল' in deadline_text
                
                post_type = None
                
                # A. শুধুমাত্র ডেডলাইন পোস্টের ডেট চেক করা হবে
                if is_deadline_post:
                    post_type = 'deadline'
                    
                    # 🎯 ডেডলাইন ডেট এক্সট্র্যাক্ট করা
                    match = re.search(r'(?:Deadline|সময়সীমা)(?:[:\s]+)?\s*(\d{1,2}\s+[A-Za-z]{3,}\s+\d{4})', deadline_text, re.IGNORECASE)
                    
                    if match:
                        date_str = match.group(1)
                        # ডেট পার্স করার জন্য বাংলা মাসের নাম সহ এনালিটিক্স যুক্ত করা
                        parsed_date = dateparser.parse(date_str, languages=['en', 'bn'])
                        
                        if parsed_date:
                            post_date = parsed_date.date()
                            
                            # 🛑 ফিল্টারিং: ডেডলাইন আজকের বা তার পরের দিন হতে হবে
                            if post_date >= today:
                                all_target_details.append({
                                    'title': title, 
                                    'url': post_url, 
                                    'deadline_text': deadline_text,
                                    'type': post_type,
                                    'parsed_date': parsed_date 
                                })
                            else:
                                print(f"        ❌ ডেট ফিল্টার: {title} বাদ দেওয়া হলো। (ডেডলাইন: {post_date})")
                                continue
                        else:
                            print(f"        ⚠️ ডেট পার্সে ব্যর্থ: {title} বাদ দেওয়া হলো।")
                            continue
                            
                # B. ফলাফল পোস্ট
                elif is_result_post:
                    post_type = 'result'
                    all_target_details.append({
                        'title': title, 
                        'url': post_url, 
                        'deadline_text': deadline_text,
                        'type': post_type,
                        'parsed_date': None 
                    })
                    
                # C. অন্যান্য পোস্ট বাদ দেওয়া 
                else:
                    print(f"        ⚠️ টাইপ ফিল্টার: {title} বাদ দেওয়া হলো (ডেডলাইন বা ফলাফল নয়)।")

    print(f"✅ পোস্টের তালিকা সংগ্রহ ও তারিখ ফিল্টারিং সম্পন্ন হয়েছে। ভ্যালিড পোস্ট পাওয়া গেছে: {len(all_target_details)} টি")
    
    final_list = all_target_details[:MAX_POSTS_TO_LOAD] 
    return final_list


# =========================================================
# ধাপ ২: সিঙ্গেল পোস্ট থেকে ইমেজ/ট্যাগ/লিঙ্ক নিষ্কাশন (আপডেটেড)
# =========================================================

def scrape_single_post_media(post_url):
    """একটি একক ব্লগ পোস্ট URL থেকে ইমেজ, লেবেল এবং আবেদনের লিংক (স্মার্ট ফলব্যাক সহ) বের করে আনে।"""
    print(f"        🔄 ধাপ ২: মিডিয়া ও লিংক ডেটা সংগ্রহ শুরু: {post_url[-40:]}...")
    media_data = {'images': [], 'download_links': [], 'labels': [], 'application_link': None, 'application_text': None} 
    
    try:
        response = requests.get(post_url, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"        ❌ একক পোস্ট রিকোয়েস্ট ব্যর্থ হয়েছে: {e}")
        return media_data

    soup = BeautifulSoup(response.text, 'html.parser') 
    
    # A. পোস্ট বডি কন্টেইনার খোঁজা
    post_body = soup.find('div', class_='post-body') 
    if not post_body:
        post_body = soup.find('div', class_='entry-content')
    if not post_body:
        return media_data

    # B. ইমেজ সংগ্রহ
    images = post_body.select('div.separator img[src], div.separator img[data-src]')
    if not images:
        images = post_body.select('img[src], img[data-src]') 

    if images:
        for img_tag in images:
            img_src = img_tag.get('src') or img_tag.get('data-src')
            if img_src:
                media_data['images'].append(img_src.replace('/s16000/', '/s1000/')) 
    else:
        print("        ❌ WARNING: কোনো ইমেজ খুঁজে পাওয়া যায়নি।")

    # 🎯 E. আবেদনের লিংক (Application Link) সংগ্রহ - (আপডেটেড লজিক) 🎯
    
    # ১. প্রথম চেষ্টা: সরাসরি 'আবেদনের লিংকঃ' টেক্সট খোঁজা (বাংলা)
    p_tags = post_body.find_all('p')
    for p_tag in p_tags:
        if 'আবেদনের লিংকঃ' in p_tag.text:
            link_tag = p_tag.find('a', href=True)
            if link_tag:
                media_data['application_link'] = link_tag['href']
                media_data['application_text'] = p_tag.text.strip()
                break 

    # ২. দ্বিতীয় চেষ্টা: যদি উপরে না পাওয়া যায়, ইংরেজি 'Apply' শব্দটি খোঁজা
    if not media_data['application_link']:
        # সব লিংক ট্যাগ খোঁজা
        all_links = post_body.find_all('a', href=True)
        
        for link in all_links:
            link_text = link.get_text().strip()
            # প্যারেন্ট এলিমেন্টের টেক্সট (যেমন: <p>Apply here: <a>Link</a></p>)
            parent_text = link.parent.get_text().strip() if link.parent else ""
            
            # কন্ডিশন ১: লিংকের নিজের টেক্সটে 'Apply' আছে কি না (যেমন: "Apply Now", "Click to Apply")
            if re.search(r'apply', link_text, re.IGNORECASE):
                media_data['application_link'] = link['href']
                media_data['application_text'] = "Apply Link: " + link_text
                print("        ℹ️ 'Apply' বাটন/লিংক টেক্সট খুঁজে পাওয়া গেছে।")
                break
            
            # কন্ডিশন ২: লিংকের ঠিক আগের বা প্যারেন্ট টেক্সটে 'Apply' আছে কি না
            # (এবং প্যারেন্ট টেক্সট খুব বড় যেন না হয়, যাতে ভুল লিংক না আসে)
            elif re.search(r'apply', parent_text, re.IGNORECASE) and len(parent_text) < 150:
                media_data['application_link'] = link['href']
                media_data['application_text'] = parent_text
                print("        ℹ️ 'Apply' টেক্সটের পাশে লিংক খুঁজে পাওয়া গেছে।")
                break

    # C. ট্যাগ/লেবেল সংগ্রহ
    labels_container = soup.find('span', class_='post-labels')
    if not labels_container:
        labels_container = soup.find('div', class_='post-footer') 
    
    if labels_container:
        label_tags = labels_container.select('a[rel="tag"]')
        if not label_tags:
            label_tags = labels_container.find_all('a') 
            
        if label_tags:
            media_data['labels'] = [tag.text.strip() for tag in label_tags if tag.text.strip()]
        
    # D. ফলব্যাক ট্যাগ
    if not media_data['labels']:
             media_data['labels'] = ['জব সার্কুলার']
            
    link_status = 'পাওয়া গেছে' if media_data['application_link'] else 'পাওয়া যায়নি'
    print(f"        ✅ মিডিয়া ও লিংক ডেটা সংগ্রহ সম্পন্ন। মোট ইমেজ: {len(media_data['images'])}, আবেদনের লিংক: {link_status}")
    return media_data

# =========================================================
# ধাপ ৪: ডুপ্লিকেট চেক ও পোস্টিং
# =========================================================

def scrape_filter_and_publish(listing_url, blogger_service, blog_id):
    """সমস্ত প্রক্রিয়া সমন্বয় করে।"""
    print("\n--- স্ক্র্যাপিং প্রক্রিয়া শুরু ---")
    
    # 1. আপনার ব্লগের সকল 'অন্যান্য' ট্যাগযুক্ত পোস্টের টাইটেল সংগ্রহ
    existing_titles = get_existing_titles(blogger_service, blog_id) 
    
    # 2. টার্গেট সাইট থেকে পোস্টের তালিকা সংগ্রহ
    all_target_details = get_all_post_links_and_details(listing_url) 
    
    if not all_target_details:
        print("পোস্টের কোনো লিংক পাওয়া যায়নি।")
        return

    # 3. ডুপ্লিকেশন চেক এবং নতুন পোস্ট ফিল্টার করা
    new_posts_to_publish = []
    
    for details in all_target_details:
        current_target_title = details['title']

        if current_target_title in existing_titles:
            print(f"⏭️ ধাপ ৪: স্কিপ করা হচ্ছে: **{current_target_title}** (ডুপ্লিকেট)")
            continue
            
        print(f"\n▶️ ধাপ ৪: নতুন পোস্ট পাওয়া গেছে ({details['type']}): {current_target_title}")
        
        media_data = scrape_single_post_media(details['url'])
        
        # 🌟 ইমেজ ফিল্টার
        if not media_data['images']:
            print(f"        ❌ IMAGE FILTER: {current_target_title} এ কোনো ইমেজ নেই, পোস্টটি বাদ দেওয়া হলো।")
            continue 

        # 🎯 ডিলিট ডেট গণনার লজিক
        delete_datetime = None
        
        if details['type'] == 'deadline' and details.get('parsed_date'):
            delete_datetime = details['parsed_date'] + timedelta(days=1)
            print(f"        ✅ ডেডলাইন পোস্টের ডিলিট ডেট গণনা করা হলো (পরের দিন): {delete_datetime.strftime('%Y-%m-%d')}")
        
        if delete_datetime is None:
            delete_datetime = datetime.now() + timedelta(days=7)
            print(f"        ⚠️ ফলব্যাক ডিলিট ডেট গণনা করা হলো (৭ দিন পর): {delete_datetime.strftime('%Y-%m-%d')}")
            
        web_end_date_tag = f"{WEB_END_DATE_TAG_PREFIX}{delete_datetime.strftime('%d-%m-%Y')}"
        print(f"        🏷️ WebEndDate ট্যাগ তৈরি: {web_end_date_tag}")

        # পোস্ট কন্টেন্ট তৈরি
        post_content = f"" 
        post_content += f"<p>ডেডলাইন/ফলাফল তথ্য: {details['deadline_text']}</p>" 
        
        # 🎯 আবেদনের লিংক যুক্ত করা (যদি থাকে)
        if media_data['application_link']:
            # টেক্সট ক্লিন করা
            app_text = media_data['application_text'] if media_data['application_text'] else "অনলাইনে আবেদন করুন"
            # 'আবেদনের লিংকঃ' শব্দটি থাকলে বাদ দেওয়া, না থাকলে যা আছে তাই রাখা
            app_text = app_text.replace('আবেদনের লিংকঃ', '').strip()
            
            application_link = media_data['application_link']
            
            post_content += f'''
            <div style="border: 2px solid #4CAF50; padding: 15px; margin: 20px 0; border-radius: 8px; background-color: #f9fff9;">
                <p style="font-weight: bold; color: #333;">আবেদনের তথ্য:</p>
                <p style="margin-top: 5px;">{app_text}</p>
                <a href="{application_link}" target="_blank" 
                   style="display: inline-block; padding: 10px 20px; text-decoration: none; 
                          background-color: #f44336; color: white; border-radius: 5px; 
                          font-weight: bold; margin-top: 10px;">
                    ➡️ অনলাইনে আবেদন করুন
                </a>
            </div>
            '''
        else:
             print("        ⚠️ আবেদনের লিংক খুঁজে পাওয়া যায়নি।")
        
        # ইমেজ এবং ডাউনলোড লিঙ্ক যোগ
        post_content += "<h3>সংযুক্ত ছবি:</h3>"
        post_content += '<div style="text-align: center;">' 
        for i, img_src in enumerate(media_data['images']):
            img_src_s1000 = img_src.replace('/s16000/', '/s1000/')
            post_content += f'<img src="{img_src_s1000}" style="max-width:100%; height:auto; margin: 10px 0;" />'
            full_res_url = img_src.replace('/s1000/', '/s16000/') 
            button_text = f"Download (Image-{i+1})"
            post_content += f'''
            <a href="{full_res_url}" download="image_{i+1}" target="_blank" 
                style="display: block; margin: 10px auto; padding: 10px 20px; text-decoration: none; 
                        background-color: #4CAF50; color: white; border-radius: 5px; width: fit-content; font-weight: bold;">
                        {button_text}
            </a>
            '''
        post_content += '</div>'
        post_content += "<p>--- তথ্যসূত্র: সরকারি চাকরি প্রস্তুতি অ্যাপ ---</p>"
        
        final_labels = media_data.get('labels', [])
        if SCRAPED_POST_TAG not in final_labels:
            final_labels.append(SCRAPED_POST_TAG)
            
        final_labels.append(web_end_date_tag)

        new_posts_to_publish.append({
            'title': current_target_title,
            'content': post_content,
            'labels': final_labels
        })

    # 4. পোস্টিং
    posts_to_publish_in_order = new_posts_to_publish[::-1]
    print(f"\n➡️ ধাপ ৪: মোট **{len(posts_to_publish_in_order)}** টি নতুন পোস্ট প্রকাশের জন্য প্রস্তুত।")

    if posts_to_publish_in_order:
        published_titles = publish_posts(blogger_service, blog_id, posts_to_publish_in_order)
        
        if published_titles:
            print(f"\n🎉 প্রক্রিয়া সম্পন্ন! {len(published_titles)} টি নতুন পোস্ট সফলভাবে প্রক্রিয়াকরণ ও প্রকাশিত হয়েছে।")
    else:
        print("পোস্ট করার জন্য কোনো নতুন ডেটা পাওয়া যায়নি।")


# =========================================================
# ধাপ ৫: publish_posts (পাবলিক পোস্টিং)
# =========================================================

def publish_posts(service, blog_id, posts_data):
    """সংগ্রহ করা পোস্ট ডেটা আপনার ব্লগে প্রকাশ করে।"""
    print("    🚀 ধাপ ৫: ব্লগারে পোস্ট করা শুরু হচ্ছে...")
    if not blog_id:
        print("ERROR: BLOG_ID পূরণ করা হয়নি।")
        return False
        
    posts_published = []
    
    for post in posts_data:
        post_body = {
            'kind': 'blogger#post',
            'title': post['title'],
            'content': post['content'],
            'labels': post['labels'], 
            'isDraft': False 
        }
        
        try:
            inserted_post = service.posts().insert(blogId=blog_id, body=post_body).execute()
            print(f"      ✅ পোস্ট সফলভাবে প্রকাশিত হয়েছে: {inserted_post['title']}") 
            posts_published.append(post['title'])
            
            time.sleep(POST_DELAY_SECONDS) 
            
        except Exception as e:
            print(f"      ❌ API ERROR: পোস্ট করার সময় ব্যর্থ হয়েছে: {post['title']}")
            print(f"      ❌ API ERROR বিবরণ: {e}")
            
            if 'quotaExceeded' in str(e):
                print("FATAL ERROR: API Quota Limit এ পৌঁছে গেছেন। 24 ঘন্টা পরে আবার চেষ্টা করুন।")
                break 

    return posts_published


# =========================================================
# ধাপ ৬: মেয়াদোত্তীর্ণ পোস্ট ডিলিট (ট্যাগ-ভিত্তিক ডিলিট)
# =========================================================

def delete_expired_posts(service, blog_id):
    """ব্লগের ট্যাগযুক্ত পোস্টগুলি চেক করে এবং মেয়াদোত্তীর্ণ হলে ডিলিট করে।"""
    print("\n--- ধাপ ৬: মেয়াদোত্তীর্ণ পোস্ট ডিলিট প্রক্রিয়া শুরু হচ্ছে (ট্যাগ-ভিত্তিক) ---")
    
    today = datetime.now().date()
    posts_deleted = 0
    
    try:
        response = service.posts().list(
            blogId=blog_id, 
            labels=SCRAPED_POST_TAG, 
            fetchBodies=False, 
            maxResults=500
        ).execute()
        
        posts = response.get('items', [])
        print(f"ℹ️ '{SCRAPED_POST_TAG}' ট্যাগযুক্ত মোট {len(posts)} টি পোস্ট ডিলিটের জন্য চেক করা হচ্ছে।")
        
        for post in posts:
            post_id = post['id']
            title = post['title']
            labels = post.get('labels', [])
            
            delete_date_str = None
            
            # 🎯 WebEndDate ট্যাগ খোঁজা
            for label in labels:
                if label.startswith(WEB_END_DATE_TAG_PREFIX):
                    date_part = label[len(WEB_END_DATE_TAG_PREFIX):]
                    try:
                        delete_date = datetime.strptime(date_part, '%d-%m-%Y').date()
                        delete_date_str = delete_date.strftime('%Y-%m-%d') 
                        break
                    except ValueError:
                        print(f"      ⚠️ ট্যাগ ডেট পার্সিং ব্যর্থ: '{label}'")
                        pass

            if delete_date_str:
                if delete_date <= today:
                    print(f"      🗑️ ডিলিট করা হচ্ছে: '{title}' (ডিলিট ডেট: {delete_date_str})")
                    service.posts().delete(blogId=blog_id, postId=post_id).execute()
                    posts_deleted += 1
                    time.sleep(DELETE_DELAY_SECONDS) 
                else:
                    print(f"      ℹ️ স্কিপ করা হচ্ছে: '{title}' (ডিলিট ডেট: {delete_date_str} > আজকের তারিখ: {today})")
            else:
                print(f"      ❌ WebEndDate ট্যাগ খুঁজে পাওয়া যায়নি, স্কিপ করা হলো: '{title}'")
            
    except Exception as e:
        print(f"❌ পোস্ট ডিলিট করার সময় ত্রুটি: {e}")

    print(f"✅ ডিলিট প্রক্রিয়া সম্পন্ন। মোট ডিলিট হয়েছে: {posts_deleted} টি পোস্ট।")


# =========================================================
# প্রধান ফাংশন (Main Function)
# =========================================================

if __name__ == '__main__':
    print("--- ধাপ ১: Blogger API সার্ভিস সেটআপ শুরু হচ্ছে ---")
    blogger_service = get_blogger_service()

    if blogger_service:
        print("✅ Blogger API সার্ভিস সেটআপ সম্পন্ন।")
        
        # 1. প্রথমে মেয়াদোত্তীর্ণ ট্যাগযুক্ত পোস্টগুলি ডিলিট করা হবে
        print("\n=== 🔄 প্রক্রিয়া শুরু: প্রথমে ডিলিট করা হচ্ছে... ===")
        delete_expired_posts(blogger_service, BLOG_ID)
        
        # 2. তারপর নতুন পোস্ট স্ক্র্যাপ, ফিল্টার এবং প্রকাশ করা হবে
        print("\n=== 🚀 প্রক্রিয়া চলমান: নতুন ডেটা সংগ্রহ ও প্রকাশ... ===")
        scrape_filter_and_publish(TARGET_LISTING_URL, blogger_service, BLOG_ID)