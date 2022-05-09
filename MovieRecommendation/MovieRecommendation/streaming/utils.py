def string_cleaning(s: str):

    s = s.strip().lower()
    s = s.replace("&nbsp;", " ")
    s = re.sub(r'<br(\s\/)?>', ' ', s)
    s = re.sub(r' +', ' ', s)  # merge multiple spaces into one

    return s 


def clean_and_tokenize(x):
    return x 


def reformat_date(x):
    x.created_at = datetime.strptime(str(x.created_at)[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
    return x

def get_model_res_count(x):
    return x 

def preprocess_youtube_comment(x):
    return x 