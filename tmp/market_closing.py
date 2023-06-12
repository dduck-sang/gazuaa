import exchange_calendars as xcals

def get_market_closing(exe_date:str):
    result = "Holiday"
    X_KRX = xcals.get_calendar("XKRX")
    o_x = X_KRX.is_session(exe_date)
    if o_x is True:
        return exe_date
    else:
        return result
