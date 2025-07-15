    pageInfo { hasNextPage endCursor }
        edges {
            node {
                id
                name
                sourceName
                retailLocation {
                id
                name
                }
                createdAt
                processedAt
                displayFinancialStatus
                displayFulfillmentStatus
                totalPriceSet { presentmentMoney { amount currencyCode } }
                totalTaxSet { presentmentMoney { amount currencyCode } }
                totalShippingPriceSet { presentmentMoney { amount currencyCode } }
                totalRefundedSet { presentmentMoney { amount currencyCode } }
                subtotalPriceSet { shopMoney { amount currencyCode } }
                totalTipReceivedSet { presentmentMoney { amount currencyCode } }
                totalReceivedSet { presentmentMoney { amount currencyCode } }
                totalDiscountsSet { presentmentMoney { amount currencyCode } }
                netPaymentSet { presentmentMoney { amount currencyCode } }
                subtotalPriceSet { presentmentMoney { amount currencyCode } }
                totalOutstandingSet { presentmentMoney { amount currencyCode } }
                
                # Essential transaction data for payment reconciliation
                transactions {
                    id
                    kind
                    gateway
                    status
                    createdAt
                    processedAt
                    test
                    amountSet { presentmentMoney { amount currencyCode } }
                }
                
                # Basic refund information
                refunds {
                    id
                    createdAt
                    refundLineItems(first: 10) {
                        nodes {
                        subtotalSet {
                            shopMoney {
                            amount
                            currencyCode
                            }
                        }
                        }
                    }
                    transactions(first: 10) {
                        nodes {
                        amountSet {
                            shopMoney {
                            amount
                            currencyCode
                            }
                        }
                        gateway
                        kind
                        status
                        processedAt
                        }
                    }
                }
            }
        }