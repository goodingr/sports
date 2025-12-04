
import pandas as pd
import logging
from src.dashboard.data import compare_model_predictions, get_cumulative_accuracy_by_model, load_forward_test_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_chart_data():
    logger.info("Testing load_forward_test_data individually...")
    models = ["ensemble", "random_forest", "gradient_boosting"]
    for model in models:
        try:
            df = load_forward_test_data(model_type=model)
            logger.info(f"Model '{model}' loaded df with shape: {df.shape}")
            if not df.empty:
                logger.info(f"Sample columns: {df.columns.tolist()[:5]}")
        except Exception as e:
            logger.error(f"Error loading model '{model}': {e}")

    logger.info("Testing compare_model_predictions...")
    try:
        # Try with default params (all leagues, all dates)
        multi_model_df = compare_model_predictions(league="all")
        logger.info(f"compare_model_predictions returned df with shape: {multi_model_df.shape}")
        if not multi_model_df.empty:
            logger.info(f"Columns: {multi_model_df.columns.tolist()}")
            logger.info(f"Sample row: {multi_model_df.iloc[0].to_dict()}")
        else:
            logger.warning("compare_model_predictions returned empty DataFrame")
            return

        logger.info("Testing get_cumulative_accuracy_by_model...")
        accuracy_df = get_cumulative_accuracy_by_model(multi_model_df)
        logger.info(f"get_cumulative_accuracy_by_model returned df with shape: {accuracy_df.shape}")
        if not accuracy_df.empty:
            logger.info(f"Columns: {accuracy_df.columns.tolist()}")
            logger.info(f"Models found: {accuracy_df['model'].unique()}")
            logger.info(f"Sample row: {accuracy_df.iloc[0].to_dict()}")
        else:
            logger.warning("get_cumulative_accuracy_by_model returned empty DataFrame")

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    test_chart_data()
