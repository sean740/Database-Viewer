import Stripe from 'stripe';

let connectionSettings: any;

async function getCredentials() {
  // First, check for a direct STRIPE_SECRET_KEY environment variable
  // This allows using WashOS's production Stripe account directly
  if (process.env.STRIPE_SECRET_KEY) {
    console.log('[STRIPE] Using STRIPE_SECRET_KEY from environment');
    return {
      publishableKey: process.env.STRIPE_PUBLISHABLE_KEY || '',
      secretKey: process.env.STRIPE_SECRET_KEY,
    };
  }

  // Fall back to Replit connector if no direct key is provided
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

  console.log('[STRIPE] Using Replit connector');
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
  
  // Track totals for debugging
  let totalBalanceChange = 0;  // Sum of ALL txn.net (actual balance change)
  let totalPayouts = 0;        // Sum of payout amounts (negative values)

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
      expand: ['data.source'],
      ...(startingAfter ? { starting_after: startingAfter } : {}),
    });
    
    totalFetched += balanceTransactions.data.length;
    console.log(`[STRIPE DEBUG] Fetched ${balanceTransactions.data.length} transactions (total: ${totalFetched}), has_more: ${balanceTransactions.has_more}`);

    for (const txn of balanceTransactions.data) {
      // Track total balance change for all transactions
      totalBalanceChange += txn.net;
      
      if (txn.type === 'charge' || txn.type === 'payment') {
        grossVolume += txn.amount;
        transactionCount++;
      } else if (txn.type === 'refund') {
        refunds += Math.abs(txn.amount);
        refundCount++;
      } else if (txn.type === 'payout') {
        // Track payouts separately - they are balance outflows but not "expenses"
        totalPayouts += txn.net;  // txn.net is negative for payouts
        console.log(`[STRIPE DEBUG] Payout to bank: ${txn.id}, net: ${txn.net / 100}`);
      } else if (txn.type === 'transfer') {
        // Check if this is a transfer to a connected account (vendor payout)
        const source = txn.source as any;
        const isConnectedAccountTransfer = source && 
          typeof source === 'object' && 
          source.object === 'transfer' && 
          source.destination &&
          typeof source.destination === 'string' &&
          source.destination.startsWith('acct_');
        
        if (isConnectedAccountTransfer) {
          console.log(`[STRIPE DEBUG] Connected account transfer: ${txn.id}, net: ${txn.net / 100}, destination: ${source.destination}`);
        } else {
          console.log(`[STRIPE DEBUG] Non-connected transfer: ${txn.id}, net: ${txn.net / 100}, destination: ${source?.destination || 'none'}`);
        }
      } else if (txn.type === 'application_fee') {
        console.log(`[STRIPE DEBUG] Application fee: ${txn.id}, net: ${txn.net / 100}`);
      }
      // All other transaction types (stripe_fee, adjustment, application_fee, etc.)
      // are already included in totalBalanceChange via txn.net
    }

    hasMore = balanceTransactions.has_more;
    if (hasMore && balanceTransactions.data.length > 0) {
      startingAfter = balanceTransactions.data[balanceTransactions.data.length - 1].id;
    }
  }
  
  // Net Volume = Total balance change + payouts (add back payouts since they're just withdrawals)
  // This gives us "money earned" rather than "balance change after payouts"
  netVolume = totalBalanceChange - totalPayouts;  // Subtract negative to add
  
  console.log(`[STRIPE DEBUG] Total balance change: ${totalBalanceChange / 100}, Total payouts: ${totalPayouts / 100}, Net Volume: ${netVolume / 100}`);

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
