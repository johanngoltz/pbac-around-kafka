import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os

def read_df(i):
	df_pbac = pd.read_csv(f"../ps/pbac/Results.{i}.csv")
	df_pbac["System Under Test"] = 'PBAC / 5M records'
	df_plain = pd.read_csv(f"../ps/plain/Results.{i}.csv")
	df_plain["System Under Test"] = 'Plain Kafka / 10M records'
	return {"pbac": df_pbac, "plain": df_plain}

def make_scatterplot(i):
	df = pd.concat(read_df(i).values())
	
	fig = px.scatter(df, x="elapsed", y="RecordsPerSecond", color="System Under Test", trendline="lowess", 
		title=f"Throughput over time, {i} producers", 
		labels={"elapsed": "Seconds elapsed", "RecordsPerSecond": "Throughput [Records/s]"})
	fig.show()

	if not os.path.exists("images"):
		os.mkdir("images")
		
	fig.write_image(f"images/fig.{i}.png")
	
def bla():
	means = []
	for i in range(6):
		i = 2 ** i
		dfs = read_df(i)
		means.append({
			"RecordsPerSecond": dfs["pbac"]["RecordsPerSecond"].mean(), 
			"ProducerCount": str(i),
			"System Under Test": "PBAC" 
		})
		means.append({		
			"RecordsPerSecond": dfs["plain"]["RecordsPerSecond"].mean(),
			"ProducerCount": str(i),
			"System Under Test": "Plain" 
		})
	
	fig = px.bar(means, x="ProducerCount", y="RecordsPerSecond", color="System Under Test", title="Throughput by #Producers", barmode="group")
	fig.show()

	if not os.path.exists("images"):
		os.mkdir("images")
		
	fig.write_image(f"images/fig.png")	
		
	
if __name__ == "__main__":
	make_scatterplot(32)
	bla()