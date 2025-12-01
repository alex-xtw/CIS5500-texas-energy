import pandas as pd
import numpy as np
import sys

def add_noise(value, noise_pct_min=0.05, noise_pct_max=0.10):
    """Add random noise to a value between noise_pct_min and noise_pct_max"""
    if pd.isna(value):
        return value

    # Random noise percentage between min and max
    noise_pct = np.random.uniform(noise_pct_min, noise_pct_max)

    # Random direction (positive or negative)
    direction = np.random.choice([-1, 1])

    # Apply noise
    noise = value * noise_pct * direction
    return value + noise

def main():
    # Read the CSV file
    input_file = 'data/ercot/ercot_load.csv'
    df = pd.read_csv(input_file)

    # Set random seed for reproducibility
    np.random.seed(42)

    # Define numeric columns (all except hour_end)
    numeric_columns = ['coast', 'east', 'far_west', 'north', 'north_c',
                      'southern', 'south_c', 'west', 'ercot']

    # Add noise to all numeric columns
    for col in numeric_columns:
        df[col] = df[col].apply(add_noise)

    # Save to CSV file
    output_file = 'data/ercot/ercot_load_predictions.csv'
    df.to_csv(output_file, index=False)

    print(f"Generated {len(df)} predictions in {output_file}")
    print(f"Sample of first 5 rows with noise applied:")
    print(df.head())

    # Calculate and display average noise applied
    original_df = pd.read_csv(input_file)
    for col in numeric_columns[:3]:  # Show for first 3 columns as examples
        avg_diff_pct = abs((df[col] - original_df[col]) / original_df[col]).mean() * 100
        print(f"\nAverage noise for {col}: {avg_diff_pct:.2f}%")

if __name__ == '__main__':
    main()
