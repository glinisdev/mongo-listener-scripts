#!/bin/bash
python3 users_listener.py &
python3 trades_listener.py &
python3 organizations_listener.py &
python3 mlscore_listener.py &
python3 loanproducts_listener.py &
python3 loanoffers_listener.py &
python3 loandeals_listener.py &
python3 loanapplication_listener.py &
python3 invoices_listener.py &
python3 cashflow_events_listener.py &
python3 cashflow_events_goals_listener.py &
python3 agribusinesses_listener.py &
python3 accounts_listener.py
