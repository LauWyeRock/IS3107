import yfinance as yf
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
import numpy as np
import pandas as pd

def predict_stock_price(ticker):
    # Download stock price data from Yahoo Finance
    stock_data = yf.download(ticker, start='2018-01-01', end='2023-04-18')
    close_prices = stock_data['Close']

    # Scale the data using MinMaxScaler
    FullData = close_prices.values.reshape(-1, 1)
    DataScaler = MinMaxScaler()
    X = DataScaler.fit_transform(FullData)

    # Create sequences of past 1000 prices
    X_samples = []
    for i in range(1000, len(X)):
        X_samples.append(X[i-1000:i])

    # Convert data to numpy arrays and reshape for LSTM input
    X_data = np.array(X_samples).reshape(-1, 1000, 1)

    # Build LSTM model
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=(1000, 1)))
    model.add(LSTM(units=50, return_sequences=True))
    model.add(LSTM(units=50))
    model.add(Dense(units=3))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(X_data, X[1000:], epochs=1, batch_size=32)

    # Use the model to predict next 3 days of prices
    last_1000_days = X[-1000:].reshape(-1, 1000, 1)
    predicted_prices = model.predict(last_1000_days)
    predicted_prices = DataScaler.inverse_transform(predicted_prices)

    # Create DataFrame with results and return it
    date_range = pd.date_range(start=stock_data.index[-1] + pd.Timedelta(days=1), periods=3, freq='D')
    results = pd.DataFrame({'Date': date_range, 'Predicted Price': predicted_prices.flatten()})
    return results





print(predict_stock_price('AAPL'))