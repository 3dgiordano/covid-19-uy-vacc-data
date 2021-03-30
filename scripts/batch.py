import pandas as pd


def main():
    df = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1ktfI1cEm-VyvRbiFkXvzTixrDrCG-85Et9Clz69QBp8/gviz/tq?tqx=out:csv&sheet=Uruguay"
    )

    df.to_csv("../data/Uruguay.csv", index=False)


if __name__ == "__main__":
    main()
