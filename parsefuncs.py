import pandas as pd
import io
import re


def parse_ping_file(data_file, COUNT_AS_LOSS_IF_OVER=-1):
    columns = ["timestamp", "icmp_seq", "time"]

    # in the data_file replace ' (' with '(' to avoid problems with the regular expression
    with open(data_file, "r") as f:
        file_string = f.read()
    file_string = file_string.replace(" (", "(")

    file_buffer = io.StringIO(file_string)

    data = pd.read_csv(
        file_buffer,
        sep=r"\[|\]|\s+",
        header=None,
        usecols=[1, 7, 9],
        names=columns,
        engine="python",
        skiprows=2,
    )
    # skip the last 3 rows
    data = data[:-3]

    # Convert time (it is unix time + microseconds as in gettimeofday)
    data["timestamp"] = data["timestamp"].apply(
        lambda x: pd.Timestamp.fromtimestamp(float(x))
    )
    data["icmp_seq"] = data["icmp_seq"].str.split("=").str[1]
    data["time"] = data["time"].str.split("=").str[1]

    # check for NaN values
    nanCounter = len(data[data.isna().any(axis=1)])
    noAnsswerCounter = len(re.findall(r".*no answer yet for icmp_seq=.*", file_string))

    print(f"Number of NaN lines: {nanCounter}")
    print(f"Number of lines with no answer: {noAnsswerCounter}")
    assert (
        nanCounter == noAnsswerCounter
    ), "Number of NaN lines and number of lines with no answer are not equal"

    data["time"] = data["time"].astype(float)

    # set all values where time is higher than 4 times std_dev to NaN
    if COUNT_AS_LOSS_IF_OVER == -1:
        std_dev = data["time"].std()
        mean = data["time"].mean()
        data["time"] = data["time"].apply(
            lambda x: float("NaN") if x > mean + 4 * std_dev else x
        )
    else:
        data["time"] = data["time"].apply(
            lambda x: float("NaN") if x > COUNT_AS_LOSS_IF_OVER else x
        )

    return data


def parse_hping_file(data_file):
    columns = ["timestamp", "icmp_seq", "time"]

    data = pd.read_csv(
        data_file,
        sep=r"\[|\]|\s+",
        header=None,
        usecols=[1, 10, 12],
        names=columns,
        engine="python",
        skiprows=1,
    )
    data = data[:-1]

    # Convert time (it is unix time + microseconds as in gettimeofday)
    data["timestamp"] = data["timestamp"].apply(
        lambda x: pd.Timestamp.fromtimestamp(float(x))
    )
    # fill icmp_seq with -1
    data["icmp_seq"] = data["icmp_seq"].str.split("=").str[1]
    # time is previous timestamp - current timestamp
    data["time"] = data["time"].str.split("=").str[1]

    # check for NaN values
    nanCounter = len(data[data.isna().any(axis=1)])
    assert nanCounter == 0, "NaN values found"

    data["time"] = data["time"].astype(float)

    # set all values where time is higher than 4 times std_dev to NaN
    print(data["time"].std())
    print(f"type: {type(data['time'].std())}")
    std_dev = data["time"].std()
    mean = data["time"].mean()
    data["time"] = data["time"].apply(
        lambda x: float("NaN") if x > mean + 4 * std_dev else x
    )

    return data


# in print you can prevent the newline with end=""
# print("Hello", "World", sep=", ")
