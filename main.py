import requests
import pandas as pd
import os
import json
import time

API_KEY = "22eeaec155e9a402b60bb4f2ec73d482"
BaseUrl= "https://api.themoviedb.org/3"
def load_data():
    data_url = "https://theone-yzse.onrender.com/views/counts"

    try:
        response = requests.get(data_url,timeout=5)
        response.raise_for_status()
        data = response.json()
        print("API call successful")

        items = data.get("items",[])
        if items :
            return items
        else :
            return print("items are not avialble")
        
    except requests.RequestException as e:
        print(f"Failed to get data: {e}")




def Json_to_csv() :
    Raw_data = load_data()
  
    script_folder = os.path.dirname(os.path.abspath(__file__))
    
  
    file_path = os.path.join(script_folder, "my_data.csv")

    contentId = [item.get("contentId") for item in Raw_data]
    contentType = [item.get("contentType") for item in Raw_data]
    view = [item.get("count") for item in Raw_data]

    Data_dict = {
        "Content ID" :contentId,
        "Content Type" : contentType,
        "Views" : view
        }
    
    os.makedirs("output",exist_ok=True)

    # file_path = os.path.join("output","my_data.csv")

    df = pd.DataFrame(Data_dict)
    # df_sorted = df.sort_values(by='Content Type', key=lambda x: x.map({'tv':0, 'movie':1}))
    # df_unique = df_sorted.drop_duplicates(subset='Content ID', keep='first')
    df.to_csv(file_path,index = False)

    print("file as saved / updated")

    return df

def get_details(contentid,contentType) :
    
    tmdb_url = BaseUrl + f"/{contentType}/{contentid}"
    # https://api.themoviedb.org/3/genre/movie/list/15512
    params = {"api_key":API_KEY}

    try :
        response = requests.get(tmdb_url,params=params,timeout=5)
        response_json = response.json()
        return response_json
    except requests.RequestException as e :
        print(f"API call failed : {e}")
        return None

def get_data():
    data_raw = Json_to_csv()
    import pandas as pd
    success_rows = []
    failed = []
    for content_id, content_type in zip(data_raw["Content ID"], data_raw["Content Type"]):
        time.sleep(2)
        try:
            metadata = get_details(content_id, content_type)
            if metadata:
                title = metadata.get("original_title") or metadata.get("name")  # movie or TV
                language = metadata.get("original_language", "")
                genres_list = metadata.get("genres", [])
                first_genre_id = genres_list[0]["id"] if genres_list else None
                success_rows.append({
                    "Content ID": content_id,
                    "Content Type": content_type,
                    "Title": title,
                    "Language": language,
                    "Genre ID": first_genre_id
                })
                print(f"{content_id}  {content_type}  {title}  {language}  {first_genre_id}")
            else:
                failed.append((content_id, content_type))
        except Exception as e:
            print(f"Error processing {content_id}, {content_type}: {e}")
            failed.append((content_id, content_type))

    max_retries = 3
    retry_count = 0
    while failed and retry_count < max_retries:
        print(f"Retrying {len(failed)} failed items, attempt {retry_count+1}")
        new_failed = []
        for content_id, content_type in failed:
            time.sleep(2)
            try:
                metadata = get_details(content_id, content_type)
                if metadata:
                    title = metadata.get("original_title") or metadata.get("name")
                    language = metadata.get("original_language", "")
                    genres_list = metadata.get("genres", [])
                    first_genre_id = genres_list[0]["id"] if genres_list else None
                    success_rows.append({
                        "Content ID": content_id,
                        "Content Type": content_type,
                        "Title": title,
                        "Language": language,
                        "Genre ID": first_genre_id
                    })
                    print(f"{content_id}  {content_type}  {title}  {language}  {first_genre_id}")
                else:
                    new_failed.append((content_id, content_type))
            except Exception as e:
                print(f"Error processing {content_id}, {content_type}: {e}")
                new_failed.append((content_id, content_type))
        failed = new_failed
        retry_count += 1


    os.makedirs("output", exist_ok=True)
    success_df = pd.DataFrame(success_rows)
    success_df.to_csv(os.path.join("output", "successful_items.csv"), index=False)


    if failed:
        print(f"Failed to process {len(failed)} items after {max_retries} retries.")
        failed_df = pd.DataFrame(failed, columns=["Content ID", "Content Type"])
        failed_df.to_csv(os.path.join("output", "failed_items.csv"), index=False)

get_data()