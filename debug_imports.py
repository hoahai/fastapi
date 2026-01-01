# debug_imports.py
import faulthandler
faulthandler.enable()

print("1) importing db_queries...")
import functions.db_queries
print("OK db_queries")

print("2) importing ggAd...")
import functions.ggAd
print("OK ggAd")

print("3) importing dataTransform...")
import functions.dataTransform
print("OK dataTransform")

print("4) importing pandas...")
import pandas as pd
print("OK pandas")
