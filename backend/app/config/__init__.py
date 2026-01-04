"""
Configuration Module
====================
Configuration settings and utilities for the application.
"""

from app.config.spark_config import get_spark, SparkConfig

__all__ = ["get_spark", "SparkConfig"]
