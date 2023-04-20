import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler 
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM

def main(ticker):
    stock_data = yf.download(ticker, start='2018-01-01', end='2023-03-20')
    close_prices = stock_data['Close']

    FullData=close_prices.values

    sc=MinMaxScaler()
    FullData = FullData.reshape(-1, 1)
    DataScaler = sc.fit(FullData)
    X=DataScaler.transform(FullData)

    X=X.reshape(X.shape[0],)

    X_samples = list()
    y_samples = list()
    
    NumberOfRows = len(X)
    TimeSteps=100
    FutureTimeSteps=3 
    
    for i in range(TimeSteps , NumberOfRows-FutureTimeSteps , 1):
        x_sample = X[i-TimeSteps:i]
        y_sample = X[i:i+FutureTimeSteps]
        X_samples.append(x_sample)
        y_samples.append(y_sample)
    
    X_data=np.array(X_samples)
    X_data=X_data.reshape(X_data.shape[0],X_data.shape[1], 1)
    
    y_data=np.array(y_samples)

    X_train = X_data
    y_train = y_data

    TimeSteps=X_train.shape[1]
    TotalFeatures=X_train.shape[2]

    regressor = Sequential()
    regressor.add(LSTM(units = 10, activation = 'relu', input_shape = (TimeSteps, TotalFeatures), return_sequences=True))
    regressor.add(LSTM(units = 5, activation = 'relu', input_shape = (TimeSteps, TotalFeatures), return_sequences=True))
    regressor.add(LSTM(units = 5, activation = 'relu', return_sequences=False ))
    regressor.add(Dense(units = FutureTimeSteps))
    regressor.compile(optimizer = 'adam', loss = 'mean_squared_error')
    regressor.fit(X_train, y_train, batch_size = 5, epochs = 100)

    
    Last100DaysPrices=np.array([close_prices[-100:]])
    Last100DaysPrices=Last100DaysPrices.reshape(-1, 1)
    X_test=DataScaler.transform(Last100DaysPrices)

    NumberofSamples=1
    TimeSteps=X_test.shape[0]
    NumberofFeatures=X_test.shape[1]
    X_test=X_test.reshape(NumberofSamples,TimeSteps,NumberofFeatures)
    Next3DaysPrice = regressor.predict(X_test)
    Next3DaysPrice = DataScaler.inverse_transform(Next3DaysPrice)
    price1 = Next3DaysPrice[0]
    price2 = Next3DaysPrice[1]
    price3 = Next3DaysPrice[2]



    df = pd.DataFrame({'ticker': [ticker], 'Predicted Day 1': [price1], 'Predicted Day 2': [price2], 'Predicted Day 3': [price3]})

    print(df)

main()