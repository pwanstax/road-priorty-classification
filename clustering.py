import pyspark.pandas as ps
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

def perform_clustering(traffy_road_prep):

    data = ps.DataFrame(traffy_road_prep)
    train_data = data.drop(['ticket_id', 'address', 'Latitude', 'Longitude', 'ID'], axis=1)

    train_data['hours_ago'] = train_data['hours_ago'].astype('float64')
    train_data['trafficCount'] = train_data['trafficCount'].astype('float64')
    train_data['accidentCount'] = train_data['accidentCount'].astype('float64')

    scaler = MinMaxScaler()
    dt = scaler.fit_transform(train_data.to_numpy())

    kmeans = KMeans(n_clusters=4, random_state = 2020)
    kmeans.fit(dt)

    # train_data['Model Label'] = kmeans.labels_.tolist()
    # data['Model Label'] = kmeans.labels_.tolist()

    # label0_df = train_data[train_data['Model Label'] == 0]
    # label1_df = train_data[train_data['Model Label'] == 1]
    # label2_df = train_data[train_data['Model Label'] == 2]
    # label3_df = train_data[train_data['Model Label'] == 3]


    return kmeans