
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
        totalPriceSet { shopMoney { amount currencyCode } }
        totalTaxSet { shopMoney { amount currencyCode } }
        totalShippingPriceSet { shopMoney { amount currencyCode } }
        totalRefundedSet { presentmentMoney { amount currencyCode } }
        subtotalPriceSet { shopMoney { amount currencyCode } }
        totalTipReceivedSet { shopMoney { amount currencyCode } }
        totalReceivedSet { presentmentMoney { amount currencyCode } }
        totalDiscountsSet { shopMoney { amount currencyCode } }
        netPaymentSet { presentmentMoney { amount currencyCode } }
        subtotalPriceSet { presentmentMoney { amount currencyCode } }
        totalOutstandingSet { presentmentMoney { amount currencyCode } }
                
        # Essential transaction data for payment reconciliation
        transactions(first: 20) {
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
  }
}
"""