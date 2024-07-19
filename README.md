# My first project using Airflow.

## Description

The pipeline will map a folder in the Docker container to a local directory of your choosing, further explained in Requirements. <br>
The pipeline will take data from the last 5 years with intervals of 1 week, and calculate their betas with respect to the market closing average, on one branch. The beta will measure the volatility of a stock with respect to a market average price. <br>
The other branch will make use of the matplotlib library to plot the prices of the stocks over the last 5 years, in order to visualise the movements of the stock over time, and how they can replicate the general market's.

## Requirements

* A Docker configuration to save the different Airflow components inside. In `Documents` > `<your-container-storage-folder>` download the docker-compose.yaml folder.
* Some slight edits on my code:
  * In `x-airflow-common` > `volumes` in **docker_compose.yaml**, you will need to elect your local directory where it says {your\\local\\directory\\here}. The opposite side of the colon (:) will be the identical folder you see in the container. The files are cloned **both ways**, so objects you have saved in the local machine in this directory will appear in the container.
  * In `x-airflow-common` > `environment` you will need to install certain Python modules using pip in the Docker container, after `_PIP_ADDITIONAL_REQUIREMENTS`. Make sure there is a space after the first hyphen in this string.

## Known Errors

1. Sometimes a zero-division error happens, which I would encourage you to fix by simply running the pipeline again. This happens because the yfinance module fails to grab data sometimes so a csv file full of NaNs causes trouble later. <br>
2. There is no need to change the memory capacity of the Docker container from the default setting. However, due to an issue with install the scikit-learn package, which I thought was initially memory-based, I created a .wslconfig file in the C:/Users/<your-user-name> which increased the memory to 8GB.<br>
This is not necessary however, as the issue was that `~ pip install sklearn` is degraded, and now `~ pip install scikit-learn` is the right version.