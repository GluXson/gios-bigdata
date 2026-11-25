"""Trening, ewaluacja i diagnostyka modeli prognozujących godzinowe PM2.5 (horyzont 24 h)."""
import numpy as np
import polars as pl
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score, root_mean_squared_error

try:
    from . import gold_features as gold
except ImportError:
    import gold_features as gold

DEFAULT_SEED = gold.DEFAULT_SEED

# Prosty zestaw 8 surowych cech (5 pogoda + 3 kalendarz) pod Iterację 1 — bez cech z M2, stacji i lagów.
BASELINE_FEATURES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "surface_pressure",
    "wind_speed_10m",
    "godzina",
    "dzien_tygodnia",
    "miesiac",
]

PM25_BIN_ORDER = ["<15", "15-35", "35-75", ">=75"]


def regression_metrics(y_true, y_pred, name, n_features):
    """Liczy MAE, RMSE i R2 dla wektorów bez braków."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    if len(y_true) == 0 or len(y_true) != len(y_pred):
        raise ValueError("y_true i y_pred muszą mieć równą, niezerową długość")
    if np.isnan(y_true).any() or np.isnan(y_pred).any():
        raise ValueError("y_true i y_pred nie mogą zawierać NaN")
    return {
        "model": name,
        "cechy": int(n_features),
        "n_test": int(len(y_true)),
        "MAE": round(float(mean_absolute_error(y_true, y_pred)), 3),
        "RMSE": round(float(root_mean_squared_error(y_true, y_pred)), 3),
        "R2": round(float(r2_score(y_true, y_pred)), 4),
    }


def evaluate_naive_baselines(train_df, test_df):
    """Naiwne punkty odniesienia: średnia z train oraz persystencja 24 h (pm25_lag24)."""
    y_test = test_df[gold.TARGET].to_numpy()
    mean_pred = np.full_like(y_test, float(train_df[gold.TARGET].mean()), dtype=float)
    dummy = regression_metrics(y_test, mean_pred, "Dummy (średnia z train)", n_features=0)

    lag24 = test_df["pm25_lag24"].to_numpy()
    valid = ~np.isnan(lag24)
    if not valid.any():
        raise ValueError("Brak niepustych wartości pm25_lag24 do oceny persystencji 24 h")
    persistence = regression_metrics(
        y_test[valid], lag24[valid], "Persystencja 24 h (pm25_lag24)", n_features=1
    )
    return [dummy, persistence]


def train_baseline(X_train, y_train, features=BASELINE_FEATURES, seed=DEFAULT_SEED):
    """Trenuje domyślny HistGradientBoostingRegressor na wskazanym podzbiorze bezpiecznych cech."""
    features = list(features)
    leaked = set(features) & gold.EXCLUDED_COLUMNS
    if leaked:
        raise ValueError(f"Wykryto niedozwolone cechy wycieku: {sorted(leaked)}")
    missing = [c for c in features if c not in X_train.columns]
    if missing:
        raise ValueError(f"Brak kolumn w X_train: {missing}")

    model = HistGradientBoostingRegressor(
        categorical_features="from_dtype",
        random_state=seed,
    )
    model.fit(X_train.select(features), y_train)
    return model


def baseline_diagnostics(test_df, y_pred, min_station_rows=500):
    """Zwraca tabele błędów wg poziomu PM2.5, kwartałów, typu stacji i konkretnych stacji."""
    y_pred = np.asarray(y_pred, dtype=float)
    if len(y_pred) != test_df.height:
        raise ValueError("Długość y_pred musi odpowiadać liczbie wierszy test_df")

    diag = test_df.select(
        "kod_stacji",
        "typ_stacji",
        "typ_obszaru",
        pl.col("data_czas_utc").dt.quarter().alias("kwartal"),
        gold.TARGET,
    ).with_columns(
        pl.Series("pred", y_pred),
        (pl.col(gold.TARGET) - pl.Series("pred", y_pred)).abs().alias("abs_err"),
        (pl.Series("pred", y_pred) - pl.col(gold.TARGET)).alias("bias"),
        pl.when(pl.col(gold.TARGET) < 15)
        .then(pl.lit("<15"))
        .when(pl.col(gold.TARGET) < 35)
        .then(pl.lit("15-35"))
        .when(pl.col(gold.TARGET) < 75)
        .then(pl.lit("35-75"))
        .otherwise(pl.lit(">=75"))
        .cast(pl.Enum(PM25_BIN_ORDER))
        .alias("przedzial_pm25"),
    )

    total = diag.height
    by_pm25_bin = (
        diag.group_by("przedzial_pm25")
        .agg(
            pl.len().alias("n"),
            (100 * pl.len() / total).round(2).alias("udzial_proc"),
            pl.col(gold.TARGET).mean().round(2).alias("srednie_pm25"),
            pl.col("pred").mean().round(2).alias("srednia_pred"),
            pl.col("abs_err").mean().round(2).alias("MAE"),
            pl.col("bias").mean().round(2).alias("bias"),
        )
        .sort("przedzial_pm25")
    )

    by_quarter = (
        diag.group_by("kwartal")
        .agg(
            pl.len().alias("n"),
            pl.col(gold.TARGET).mean().round(2).alias("srednie_pm25"),
            pl.col("pred").mean().round(2).alias("srednia_pred"),
            pl.col("abs_err").mean().round(2).alias("MAE"),
        )
        .sort("kwartal")
    )

    by_station_type = (
        diag.group_by("typ_stacji")
        .agg(
            pl.len().alias("n"),
            pl.col(gold.TARGET).mean().round(2).alias("srednie_pm25"),
            pl.col("pred").mean().round(2).alias("srednia_pred"),
            pl.col("abs_err").mean().round(2).alias("MAE"),
            pl.col("bias").mean().round(2).alias("bias"),
        )
        .sort("MAE", descending=True)
    )

    stations_all = (
        diag.group_by("kod_stacji", "typ_stacji")
        .agg(
            pl.len().alias("n"),
            pl.col(gold.TARGET).mean().round(2).alias("srednie_pm25"),
            pl.col("pred").mean().round(2).alias("srednia_pred"),
            pl.col("abs_err").mean().round(2).alias("MAE"),
        )
        .sort("MAE", descending=True)
    )
    stations = stations_all.filter(pl.col("n") >= min_station_rows)
    if stations.is_empty():
        stations = stations_all

    return {
        "by_pm25_bin": by_pm25_bin,
        "by_quarter": by_quarter,
        "by_station_type": by_station_type,
        "stations": stations,
    }


def run_iteration1(gold_dir=gold.GOLD_DIR, seed=DEFAULT_SEED):
    """Wczytuje warstwę Gold, trenuje model bazowy (8 cech) i wylicza metryki + diagnostykę."""
    train_df, test_df, meta = gold.load_gold(gold_dir=gold_dir, return_frames=True)
    X_train, y_train = gold.to_xy(train_df)
    X_test, y_test = gold.to_xy(test_df)

    model = train_baseline(X_train, y_train, features=BASELINE_FEATURES, seed=seed)
    y_pred = model.predict(X_test.select(BASELINE_FEATURES))

    rows = [
        *evaluate_naive_baselines(train_df, test_df),
        regression_metrics(
            y_test,
            y_pred,
            "Iteracja 1 — Baseline (HGBR, 8 cech)",
            n_features=len(BASELINE_FEATURES),
        ),
    ]
    return {
        "model": model,
        "y_test": y_test,
        "y_pred": y_pred,
        "metrics": pl.DataFrame(rows),
        "diagnostics": baseline_diagnostics(test_df, y_pred),
        "metadata": meta,
    }


def main():
    result = run_iteration1()
    print(result["metrics"])
    print("\nBłąd wg przedziału PM2.5:")
    print(result["diagnostics"]["by_pm25_bin"])
    print("\nBłąd wg kwartału:")
    print(result["diagnostics"]["by_quarter"])
    return result


if __name__ == "__main__":
    main()
