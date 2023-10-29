### MAIN PROGRAM ###
def main(source, input_filename, output_filename, save_option):
    # import libraries
    import pandas as pd
    from bertopic import BERTopic
    from sentence_transformers import SentenceTransformer
    from hdbscan import HDBSCAN
    from bertopic.representation import KeyBERTInspired
    from sklearn.feature_extraction.text import CountVectorizer
    import pickle
    import glob, os
    import configparser

    # get config settings
    config_file = '../config.ini'
    settings = configparser.ConfigParser(inline_comment_prefixes="#")
    settings.read(config_file)

    modelling_path = '../data/modelling/'

    ### Input data ###
    # load full dataset
    full_df = pd.DataFrame()
    path = settings['DEFAULT']['processed_data_folder'] + settings['LENS_API.PATENTS']['subfolder'] + '*_data.parquet'
    files = glob.glob(path)
    for file in files:
        df = pd.read_parquet(file)
        full_df = pd.concat([full_df, df])
    full_df['lens_id'] = full_df['lens_id'].astype('string')

    # load labelled data
    labelled_df = pd.DataFrame()
    path = settings['DEFAULT']['filtered_data_folder'] + settings['LENS_API.PATENTS']['subfolder'] + '*_data_filtered.csv'
    files = glob.glob(path)
    for file in files:
        df = pd.read_csv(file)
        labelled_df = pd.concat([labelled_df, df])

    labelled_df['tech'] = labelled_df[['quantum', 'semiconductors', 'cell-based meats', 'hydrogen power', 'personalised medicine']].idxmax(1)
    labelled_df['tech'] = pd.factorize(labelled_df['tech'])[0] + 1
    labelled_df['lens_id'] = labelled_df['lens_id'].astype('string')
    # join labels to full dataset
    joined_df = full_df.set_index('lens_id').join(labelled_df.set_index('lens_id'), rsuffix='_join', how='left')
    joined_df['tech'] = joined_df['tech'].fillna(-1)

    # create doc text and target classes lists 
    docs = joined_df['title'].to_list()
    target_classes = joined_df['tech'].astype('int').to_list()

    ### Define topic model ###
    # define model components
    sentence_model = SentenceTransformer("all-MiniLM-L6-v2")
    hdbscan_model = HDBSCAN(min_cluster_size=150, prediction_data=True)
    representation_model = KeyBERTInspired()
    # define topic model
    topic_model = BERTopic(embedding_model=sentence_model, hdbscan_model=hdbscan_model, representation_model=representation_model,
                        top_n_words=10, nr_topics='auto', calculate_probabilities=False)
    
    ### Run topic modelling ###
    # compute embeddings
    embeddings = sentence_model.encode(docs, show_progress_bar=True)
    # store as pickle file
    with open(os.path.join(modelling_path,'patent_title_embeddings'), 'wb') as f:
        pickle.dump(embeddings, f)
    # fit and transform model
    topics, probs = topic_model.fit_transform(docs, embeddings, y=target_classes)
    # represent topics
    vectorizer_model = CountVectorizer(stop_words="english", ngram_range=(1, 2))
    topic_model.update_topics(docs, vectorizer_model=vectorizer_model)
    # save model
    topic_model.save(os.path.join(modelling_path,'patent_title_model'), serialization='safetensors', save_ctfidf=True, save_embedding_model=sentence_model)
    # save outputs
    with open(os.path.join(modelling_path,'patent_title_topics'), 'wb') as f:
        pickle.dump(topics, f)
    with open(os.path.join(modelling_path,'patent_title_probs'), 'wb') as f:
        pickle.dump(probs, f)

    ### Output data ###
    # create a topic docs dataframe
    topic_docs_df = joined_df.copy()
    topic_docs_df['topic_number'] = topics
    topic_docs_df['topic_probabilities'] = probs
    # save as csv
    topic_docs_df.to_csv('../data/dashboard/patent_title_topic_docs.csv')
    
    # create a topic names dataframe
    topic_names_df = topic_model.get_topic_info()
    top_terms = (topic_model.get_topics().values())
    topic_names_df['topic_terms'] = [[pair[0] for pair in topic] for topic in top_terms]
    topic_names_df['term_probabilities'] = [[float(pair[1]) for pair in topic] for topic in top_terms]
    # save as csv and display
    topic_names_df.to_csv('../data/dashboard/patent_title_topic_names.csv')
    topic_names_df

    return

### SCRIPT TO RUN WHEN CALLED STANDALONE ###
if __name__=='__main__':
    # input arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', help='choose from GDELT, LENS_API.PATENTS, LENS_API.JOURNALS')
    parser.add_argument('--save', default=None, type=str, help = "value determines how the data will be saved. See config.ini for default and valid options")
    args = parser.parse_args()

    # run main
    main(args.source, args.save)