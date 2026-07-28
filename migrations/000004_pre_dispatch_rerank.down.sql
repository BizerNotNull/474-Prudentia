UPDATE scheduler_reservations
SET state = 'given_up', updated_at = clock_timestamp()
WHERE state = 'abandoned_rerank';

ALTER TABLE scheduler_reservations
    DROP CONSTRAINT scheduler_reservations_state_check,
    ADD CONSTRAINT scheduler_reservations_state_check CHECK (
        state IN ('reserved', 'dispatch_authorized', 'released', 'given_up', 'orphaned')
    );
