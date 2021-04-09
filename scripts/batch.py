import pandas as pd

base_seet = "https://docs.google.com/spreadsheets/d/" \
            "1ktfI1cEm-VyvRbiFkXvzTixrDrCG-85Et9Clz69QBp8/gviz/tq?tqx=out:csv&sheet="


def uruguay():
    df = pd.read_csv(
        f"{base_seet}Uruguay"
    )

    df.to_csv("../data/Uruguay.csv", index=False)


def regions():
    df = pd.read_csv(
        f"{base_seet}Regions"
    )

    df.to_csv("../data/Regions.csv", index=False)


def segments():
    df = pd.read_csv(
        f"{base_seet}Segment"
    )

    df.to_csv("../data/Segments.csv", index=False)


def main():
    uruguay()
    regions()
    segments()


if __name__ == "__main__":
    main()
