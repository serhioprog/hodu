import asyncio
from loguru import logger
from src.services.engine_v2.engine import HybridEngine
from src.database.db import async_session_maker


async def main():
    logger.info('[MANUAL] starting engine v2 trigger')
    engine_v2 = HybridEngine.build_default()
    async with async_session_maker() as session:
        report = await engine_v2.run_full_dedup(session)
        await session.commit()

    print('')
    print('=== ENGINE 2 MANUAL TRIGGER COMPLETE ===')
    print(f'new_clusters_proposed:  {report.new_clusters_proposed}')
    print(f'attached_clusters:      {report.attached_clusters_count}')
    print(f'pairs_scored:           {report.pairs_scored}')
    print(f'pairs_cached:           {report.pairs_cached}')
    print(f'bridge_blocks:          {report.bridge_blocks}')
    print(f'approved_disagreements: {report.approved_disagreements}')
    print(f'errors_count:           {report.errors_count}')
    print(f'cost_usd:               ${report.cost_usd:.4f}')
    print(f'latency_ms:             {report.latency_ms}')


asyncio.run(main())

