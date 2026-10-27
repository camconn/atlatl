"""`python -m dbt_atlatl sources …`; maturin ships no console script beside a binary."""

import sys

from dbt_atlatl._cli import main

sys.exit(main())
