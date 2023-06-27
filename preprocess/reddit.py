import pandas as pd
import os
import logging
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

data_folder = 'C:/Data/Reddit'
logs_filename = 'C:/Data/Logs/reddit_preprocess.log'
logs_level = logging.INFO

def main():
    def compute_cripto_summaries(crypto_name):
        def already_computed(destination_file):
            already_exists = os.path.exists(destination_file)

            if already_exists:
                logging.debug(f"{destination_file} already exists")

            return already_exists
        
        def get_summary_path(path):
            return os.path.join(path, 'summary')

        def compute_submission_file_summary(submission_path, file):
            summary_path = get_summary_path(submission_path)
            os.makedirs(summary_path, exist_ok=True)

            destination_file = os.path.join(summary_path, f'summary_{file}')
            if already_computed(destination_file):
                return pd.read_csv(destination_file)

            file_path = os.path.join(submission_path, file)
            data = pd.read_csv(file_path)

            data['created_utc'] = pd.to_datetime(data['created_utc'], unit='s')
            data['day'] = data['created_utc'].dt.date

            data['text'] = data['title'].fillna('') + ' - ' + data['selftext'].fillna('')
            data['sentiment'] = data['text'].apply(lambda text: analyzer.polarity_scores(text)['compound'])

            grouped = data.groupby('day').agg(
                submissions_count=('id', 'count'),
                submissions_average_score=('score', 'mean'),
                submissions_average_upvote_ratio=('upvote_ratio', 'mean'),
                submissions_average_sentiment=('sentiment', 'mean'),
            )

            grouped.to_csv(destination_file)
            logging.info(f"Saved preprocess output to file {destination_file}. It has {len(grouped)} rows")

            return grouped
        
        def compute_comment_file_summary(comment_path, file):
            summary_path = get_summary_path(comment_path)
            os.makedirs(summary_path, exist_ok=True)

            destination_file = os.path.join(summary_path, f'summary_{file}')
            if already_computed(destination_file):
                return pd.read_csv(destination_file)

            file_path = os.path.join(comment_path, file)
            data = pd.read_csv(file_path)

            data['created_utc'] = pd.to_datetime(data['created_utc'], unit='s')
            data['day'] = data['created_utc'].dt.date

            data['body'] = data['body'].fillna('')
            data['sentiment'] = data['body'].apply(lambda text: analyzer.polarity_scores(text)['compound'])

            grouped = data.groupby('day').agg(
                comments_count=('id', 'count'),
                comments_average_score=('score', 'mean'),
                comments_average_controversiality=('controversiality', 'mean'),
                comments_average_sentiment=('sentiment', 'mean'),
            )

            grouped.to_csv(destination_file)
            logging.info(f"Saved preprocess output to file {destination_file}. It has {len(grouped)} rows")

            return grouped

        def compute_all_summary(path, compute_summary):
            # List all csv files in the subdirectory
            files = [file for file in os.listdir(path) if file.endswith('.csv')]
            logging.info(f"Found {len(files)} files inside {path}")

            all_data = pd.DataFrame()
            for file in files:
                summary = compute_summary(path, file)
                all_data = pd.concat([all_data, summary])
            
            return all_data

        subdir_path = os.path.join(data_folder, crypto_name)
        submission_path = os.path.join(subdir_path, 'submission')
        comment_path = os.path.join(subdir_path, 'comment')
        
        all_submission_data = compute_all_summary(submission_path, compute_submission_file_summary)
        all_comment_data = compute_all_summary(comment_path, compute_comment_file_summary)

        all_data = all_submission_data.merge(all_comment_data, on='day')

        destination_file = os.path.join(subdir_path, f'overall_summary.csv')
        all_data.to_csv(destination_file)
        logging.info(f"Saved overall preprocess output to file {destination_file}. It has {len(all_data)} rows")

    logging.info("Starting reddit preprocess.")

    analyzer = SentimentIntensityAnalyzer()

    subdirs = [d for d in os.listdir(data_folder) if os.path.isdir(os.path.join(data_folder, d))]
    for subdir in subdirs:
        compute_cripto_summaries(subdir)

if __name__ == "__main__":
    logging.basicConfig(filename=logs_filename, level=logs_level, format='%(asctime)s - %(levelname)s - %(message)s')
    main()