package com.pipeline.identity;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.UUID;

public class IdentityResolutionFunction
        extends KeyedProcessFunction<String, RawEvent, UnifiedEvent> {

    static final OutputTag<MergeEvent> MERGE_TAG =
            new OutputTag<MergeEvent>("identity-merges") {};

    private transient MapState<String, String> anonToCanonical;
    private transient MapState<String, String> userToCanonical;

    @Override
    public void open(Configuration parameters) {
        anonToCanonical = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("anon-to-canonical", String.class, String.class));
        userToCanonical = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("user-to-canonical", String.class, String.class));
    }

    @Override
    public void processElement(RawEvent event, Context ctx, Collector<UnifiedEvent> out)
            throws Exception {

        String anonId = event.anonymousId;
        String userId = event.hasUserId() ? event.userId : null;

        String anonCanonical = anonToCanonical.get(anonId);

        String canonicalId;

        if (userId != null) {
            String userCanonical = userToCanonical.get(userId);

            if (anonCanonical != null && userCanonical != null) {
                if (!anonCanonical.equals(userCanonical)) {
                    // MERGE: two canonical IDs for the same person
                    // The user_id's canonical wins (it was established at first sign-in)
                    String winner = userCanonical;
                    String loser = anonCanonical;
                    anonToCanonical.put(anonId, winner);

                    ctx.output(MERGE_TAG, MergeEvent.of(
                            loser, winner, event.tenantId, event.eventTime));

                    canonicalId = winner;
                } else {
                    canonicalId = anonCanonical;
                }
            } else if (anonCanonical != null) {
                // First sign-in: link user_id to the existing canonical
                userToCanonical.put(userId, anonCanonical);
                canonicalId = anonCanonical;
            } else if (userCanonical != null) {
                // New device for a known user
                anonToCanonical.put(anonId, userCanonical);
                canonicalId = userCanonical;
            } else {
                // Brand new user, first event is already signed in
                canonicalId = generateCanonicalId();
                anonToCanonical.put(anonId, canonicalId);
                userToCanonical.put(userId, canonicalId);
            }
        } else {
            // Anonymous visitor
            if (anonCanonical != null) {
                canonicalId = anonCanonical;
            } else {
                canonicalId = generateCanonicalId();
                anonToCanonical.put(anonId, canonicalId);
            }
        }

        out.collect(UnifiedEvent.from(event, canonicalId));
    }

    private String generateCanonicalId() {
        return UUID.randomUUID().toString();
    }
}
