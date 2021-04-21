# Large Scale Data Processing: Project 3
## Group Members: Hengrui Xie, Yinzhe Ma

1. Results are shown below

|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes        |

2. Informaiton required are shown below and MIS result files are in the result folder above.

|        Graph file       |           # of Iterations    | Run Time   | Verified |
| ----------------------- | ---------------------------- | ---------- |----------|
| small_edges.csv         |            1                 |    1s      |   Yes    |
| line_100_edges.csv      |            4                 |    2s      |   Yes    |
| twitter_100_edges.csv   |            8                 |    3s      |   Yes    |
| twitter_1000_edges.csv  |            10                |    4s      |   Yes    |
| twitter_10000_edges.csv |            13                |    13s     |   Yes    |


3. **(3 points)**  
(We figured out the program last minute and everything works perfectly fine locally. However, we had trouble using GCP to run the twitter_original_edges.csv)
