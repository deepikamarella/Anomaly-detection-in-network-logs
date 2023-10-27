import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import OPTICS
import matplotlib.pyplot as plt
from numpy import quantile
from sklearn.model_selection import train_test_split

# Load log file data into a DataFrame
log_data = pd.read_csv(r'C:\Users\Sudheer\Downloads\pichiproject\py_anomaly_updated\py_anomaly\.py_anomaly\Dataset\plot_dataset.csv')

# Preprocess the data if needed

# Select the feature(s) for anomaly detection
features = ['EventID', 'Occurrences']  # Replace with the actual feature columns



# Perform feature scaling
scaler = StandardScaler()
scaled_data = scaler.fit_transform(log_data[features])

# Create the OPTICS clustering model
model = OPTICS(min_samples=5, xi=0.05, min_cluster_size=0.1)

# Fit the model to the scaled data
model.fit(scaled_data)

# Retrieve the reachability distance and cluster labels
reachability = model.reachability_
labels = model.labels_

# Set a threshold to determine anomalies using training data
thresh = quantile(reachability, 0.85)

# Split the data into training and testing sets
# Specify the random_state for reproducibility
train_data, test_data, train_labels, test_labels = train_test_split(log_data, labels, test_size=0.15, random_state=42)

# Create a dictionary to map cluster labels to unique shapes and colors
cluster_colors = {
    0: {'color': 'teal', 'marker': 'o', 'label': 'Cluster 0'},
    1: {'color': 'pink', 'marker': '^', 'label': 'Cluster 1'},
    -1: {'color': 'purple', 'marker': 'x', 'label': 'Anomalies'}
}

# Plot the clusters for the training data
plt.figure(figsize=(8, 6))
for cluster_label in np.unique(train_labels):
    cluster_indices = np.where(train_labels == cluster_label)[0]
    cluster_reachability = reachability[cluster_indices]

    plt.scatter(cluster_indices, cluster_reachability, c=cluster_colors[cluster_label]['color'],
                marker=cluster_colors[cluster_label]['marker'], label=cluster_colors[cluster_label]['label'], s=100)
plt.xlabel('Data Point', fontsize=15)
plt.ylabel('Reachability Distance (Training Data)', fontsize=15)
plt.title('Cluster Formation with OPTICS (Training Data)')
plt.legend()
plt.show()


test_reachability = model.fit_predict(scaler.transform(test_data[features]))

# Identify the anomalies in the testing data based on the threshold
test_anomalies = test_data[test_reachability >= thresh]

# Plot the anomalies in the testing data
plt.scatter(test_data['EventID'], test_data['Occurrences'], marker='^', c='blue', label='Normal')
plt.scatter(test_anomalies['EventID'], test_anomalies['Occurrences'], marker='s', c='red', label='Anomalies')

plt.xlabel('Feature1', fontsize=15, fontweight='bold')
plt.ylabel('Feature2', fontsize=15, fontweight='bold')
plt.title('Anomaly detection')
plt.legend()
plt.show()

# Print the detected anomalies in the testing data
print(test_anomalies)
