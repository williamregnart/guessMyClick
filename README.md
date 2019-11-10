# guessMyClick

##  Description

This program create a model to predict if the user clicks or not from a dataset of advertising data.
The model is already saved you have just to give your data and the trained model will be apply on it.
In input the program takes a JSON file. 
In output it gives a CSV with the first column which contains the predicted values.

## Instructions to launch the program

To launch the program follow the instructions: 
* Clone the current git repository.
* In the terminal, move to the root folder of the cloned project.
* Do the command `sbt assembly` to create the JAR file. 
* A file named `guessMyClick.jar` is created in `target/scala-2.12`. Move it to the root directory (it needs to be a the same level than the `model` folder)
* Launch with `java -jar guessMyClick.jar dataset.json [--DEBUG] [--TRAIN]`
`dataset.json` is the dataset you want to run prediction onto.
`--DEBUG` show debug information
`--TRAIN` trains a new model on the given dataset

## Train a new model

Please make sure that if a folder named `models` exists, you delete it beforehand.

Then, train a new model by calling `java -jar guessMyClisk.jar dataset.json --TRAIN`. It should take a few minutes, and creates a folder named `model`. The metrics for the trained model are shown during the process.

## Predict

The prediction requires a model to be created beforehand; the folder `model` must exist and it must not be empty.
If you have an `output` folder, please delete it before running the prediction.
To predict the outcome of input values, run `java -jar guessMyClick.jar dataset.json`.
Please note that if your data contains a `label` attribute, it will be replaced during the process by predicted values.

The results are stored in a folder called `output`. Inside, you will find some files, namely one CSV containing the results. The CSV's name changes because of Spark implementation of workers, but it should always follow the scheme `part-0000-xxxx.csv`.
 
The predicted label is stored in the first column, called label, and the value varies from true to false.