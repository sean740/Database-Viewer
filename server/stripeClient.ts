import Stripe from 'stripe';

let connectionSettings: any;

async function getCredentials() {
  const hostname = process.env.REPLIT_CONNECTORS_HOSTNAME;
  const xReplitToken = process.env.REPL_IDENTITY
    ? 'repl ' + process.env.REPL_IDENTITY
    : process.env.WEB_REPL_RENEWAL
      ? 'depl ' + process.env.WEB_REPL_RENEWAL
      : null;

  if (!xReplitToken) {
    throw new Error('X_REPLIT_TOKEN not found for repl/depl');
  }

  const connectorName = 'stripe';
  const isProduction = process.env.REPLIT_DEPLOYMENT === '1';
  const targetEnvironment = isProduction ? 'production' : 'development';

  const url = new URL(`https://${hostname}/api/v2/connection`);
  url.searchParams.set('include_secrets', 'true');
  url.searchParams.set('connector_names', connectorName);
  url.searchParams.set('environment', targetEnvironment);

  const response = await fetch(url.toString(), {
    headers: {
      'Accept': 'application/json',
      'X_REPLIT_TOKEN': xReplitToken
    }
  });

  const data = await response.json();
  
  connectionSettings = data.items?.[0];

  if (!connectionSettings || (!connectionSettings.settings.publishable || !connectionSettings.settings.secret)) {
    throw new Error(`Stripe ${targetEnvironment} connection not found`);
  }

  return {
    publishableKey: connectionSettings.settings.publishable,
    secretKey: connectionSettings.settings.secret,
  };
}

export async function getStripeClient(): Promise<Stripe> {
  const { secretKey } = await getCredentials();
  return new Stripe(secretKey, {
    apiVersion: '2025-11-17.clover',
  });
}

export interface StripeWeeklyMetrics {
  grossVolume: number;
  netVolume: number;
  refunds: number;
  disputes: number;
  transactionCount: number;
  refundCount: number;
  disputeCount: number;
}

export async function getStripeMetricsForWeek(
  startTimestamp: number,
  endTimestamp: number
): Promise<StripeWeeklyMetrics> {
  const stripe = await getStripeClient();
  
  let grossVolume = 0;
  let netVolume = 0;
  let refunds = 0;
  let disputes = 0;
  let transactionCount = 0;
  let refundCount = 0;
  let disputeCount = 0;

  // Debug: Log the date range being queried
  console.log(`[STRIPE DEBUG] Querying balance transactions from ${new Date(startTimestamp * 1000).toISOString()} to ${new Date(endTimestamp * 1000).toISOString()}`);

  let hasMore = true;
  let startingAfter: string | undefined;
  let totalFetched = 0;

  while (hasMore) {
    const balanceTransactions = await stripe.balanceTransactions.list({
      created: {
        gte: startTimestamp,
        lt: endTimestamp,
      },
      limit: 100,
      ...(startingAfter ? { starting_after: startingAfter } : {}),
    });
    
    totalFetched += balanceTransactions.data.length;
    console.log(`[STRIPE DEBUG] Fetched ${balanceTransactions.data.length} transactions (total: ${totalFetched}), has_more: ${balanceTransactions.has_more}`);

    for (const txn of balanceTransactions.data) {
      if (txn.type === 'charge' || txn.type === 'payment') {
        grossVolume += txn.amount;
        netVolume += txn.net;
        transactionCount++;
      } else if (txn.type === 'refund') {
        refunds += Math.abs(txn.amount);
        refundCount++;
        netVolume += txn.net;
      } else if (txn.type === 'stripe_fee' || txn.type === 'adjustment') {
        netVolume += txn.net;
      }
    }

    hasMore = balanceTransactions.has_more;
    if (hasMore && balanceTransactions.data.length > 0) {
      startingAfter = balanceTransactions.data[balanceTransactions.data.length - 1].id;
    }
  }

  let disputeHasMore = true;
  let disputeStartingAfter: string | undefined;

  while (disputeHasMore) {
    const listParams: Stripe.DisputeListParams = {
      created: {
        gte: startTimestamp,
        lt: endTimestamp,
      },
      limit: 100,
    };
    if (disputeStartingAfter) {
      listParams.starting_after = disputeStartingAfter;
    }
    
    const disputesList = await stripe.disputes.list(listParams);

    for (const dispute of disputesList.data) {
      disputes += dispute.amount;
      disputeCount++;
    }

    disputeHasMore = disputesList.has_more;
    if (disputeHasMore && disputesList.data.length > 0) {
      disputeStartingAfter = disputesList.data[disputesList.data.length - 1].id;
    }
  }

  return {
    grossVolume: grossVolume / 100,
    netVolume: netVolume / 100,
    refunds: refunds / 100,
    disputes: disputes / 100,
    transactionCount,
    refundCount,
    disputeCount,
  };
}

export async function checkStripeConnection(): Promise<boolean> {
  try {
    const stripe = await getStripeClient();
    await stripe.balance.retrieve();
    return true;
  } catch (error) {
    console.error('Stripe connection check failed:', error);
    return false;
  }
}
