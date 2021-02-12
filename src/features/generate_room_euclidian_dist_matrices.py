#%%
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
import re

# get all label csv files
label_csv_files = list(Path("./data/processed/cad_maps/").glob("**/*_labels.csv"))

pbar = tqdm(label_csv_files)
for csv_file in pbar:
    csv_file_path = str(csv_file)
    pbar.set_description(f"Processing {csv_file_path}...")

    pattern = "^building_(.+)_floor_(.+)_labels\\.csv$"

    match = re.search(pattern, csv_file.name)
    if match is not None:
        # if extraction was successful, read metric df and compose tuple
        building_id, floor_id = match.groups()
    else:
        print(f"Weird file {csv_file_path}")
        continue

    floor_labels = pd.read_csv(csv_file_path, index_col=0)

    floor_plot_labels = floor_labels[floor_labels["Label Type"] == "Room ID"]
    floor_plot_labels["Label Text"].to_list()

    labels = floor_plot_labels["Label Text"].to_list()
    label_x_coords = floor_plot_labels["X-coordinate"].to_numpy()
    label_y_coords = floor_plot_labels["Y-coordinate"].to_numpy()

    room_qty = len(labels)
    room_distances = np.zeros([room_qty, room_qty])

    for i in range(room_qty):
        for j in range(room_qty):
            room_distances[i, j] = np.linalg.norm(np.array([label_x_coords[i], label_y_coords[i]]) - np.array([label_x_coords[j], label_y_coords[j]]))

    room_distance_df = pd.DataFrame(room_distances)
    room_distance_df.columns = labels
    room_distance_df["Room"] = labels
    room_distance_df.set_index("Room", inplace=True)
    room_distance_df.to_csv(f"./data/processed/cad_maps/building_{building_id}_floor_{floor_id}_euclidian_distances.csv")

