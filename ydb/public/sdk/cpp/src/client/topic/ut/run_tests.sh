#!/bin/bash

~/ydbwork/ydb/ya test . \
    --test-filter="TxUsage::Sinks_Olap_WriteToTopicAndTable_1_Table" \
    --test-filter="TxUsage::Sinks_Olap_WriteToTopicAndTable_1_Query" \
    --test-filter="TxUsage::Sinks_Olap_WriteToTopicAndTable_2_Table" \
    --test-filter="TxUsage::Sinks_Olap_WriteToTopicAndTable_2_Query" \
    --test-filter="TxUsage::Sinks_Olap_WriteToTopicAndTable_4_Table" \
    --test-filter="TxUsage::Sinks_Olap_WriteToTopicAndTable_4_Query" \
    --build=relwithdebinfo -P
