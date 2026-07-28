ALTER TABLE scheduler_reservations
    DROP CONSTRAINT scheduler_reservations_state_check,
    ADD CONSTRAINT scheduler_reservations_state_check CHECK (
        state IN ('reserved', 'abandoned_rerank', 'dispatch_authorized', 'released', 'given_up', 'orphaned')
    );
