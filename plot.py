import matplotlib.pyplot as plt

# Data for the graph
dataset_sizes = [1186, 2156, 6066, 13525]
execution_times_map_reduce = [1.026308, 1.067593, 1.249185, 1.598612]
execution_times_dataframe = [0.944561, 1.126407, 1.066844, 1.187462]
labels = ['Jan 2022', 'Feb 2022', 'Mar 2022', 'Apr 2022']

# Create the plot
plt.figure(figsize=(10, 6))

# Plot for map_reduce_style
plt.plot(dataset_sizes, execution_times_map_reduce, marker='o', linestyle='-', color='b', label='Map Reduce Style')

# Plot for dataframe_style
plt.plot(dataset_sizes, execution_times_dataframe, marker='o', linestyle='-', color='r', label='DataFrame Style')

# Annotate the points with the month labels for map_reduce_style
for i, label in enumerate(labels):
    plt.text(dataset_sizes[i], execution_times_map_reduce[i], label, fontsize=10, ha='right', color='blue')

# Annotate the points with the month labels for dataframe_style
for i, label in enumerate(labels):
    plt.text(dataset_sizes[i], execution_times_dataframe[i], label, fontsize=10, ha='right', color='red')

# Adding titles and labels
plt.title('Execution Time vs Dataset Size')
plt.xlabel('Dataset Size')
plt.ylabel('Execution Time (seconds)')

# Display the legend
plt.legend()

# Display the grid
plt.grid(True)

# Show the plot
plt.show()
