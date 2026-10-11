"""CLI command to generate deterministic synthetic transactional datasets."""

import argparse
import sys
from pathlib import Path

# Add src to pythonpath
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root / "src"))

from ecommerce_data_lake.config import PipelineConfig
from ecommerce_data_lake.data_generation import SyntheticDataGenerator
from ecommerce_data_lake.logging_config import setup_logger

logger = setup_logger("scripts.generate_data")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce datasets.")
    parser.add_argument("--batch", type=str, default="batch_001", help="Batch identifier (default: batch_001)")
    parser.add_argument("--profile", type=str, default="dev", choices=["dev", "standard", "large"], help="Profile size")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for deterministic generation")
    parser.add_argument("--output-dir", type=str, default="./data/raw", help="Target raw output directory")
    parser.add_argument("--incremental", action="store_true", help="Generate incremental batch")
    args = parser.parse_args()

    cfg = PipelineConfig.load()
    profile = cfg.generator.profiles.get(args.profile, cfg.generator.profiles["dev"])
    generator = SyntheticDataGenerator(seed=args.seed)

    logger.info(f"Generating batch '{args.batch}' using profile '{args.profile}' (seed={args.seed}, incremental={args.incremental})...")

    batch_data = generator.generate_batch(
        profile=profile,
        batch_id=args.batch,
        is_incremental=args.incremental,
    )

    out_path = Path(args.output_dir)
    file_paths = generator.write_batch_to_csv(batch_data, out_path, args.batch)

    logger.info(f"Batch '{args.batch}' generated successfully with {len(file_paths)} files:")
    for entity, fpath in file_paths.items():
        count = len(batch_data[entity])
        logger.info(f"  - {entity}: {count} records -> {fpath}")


if __name__ == "__main__":
    main()
