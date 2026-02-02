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
      } else if (txn.type === 'application_fee') {
        // Application fees are the platform's cut from Connect charges
        // These should be added to net volume
        netVolume += txn.net;
        console.log(`[STRIPE DEBUG] Added application fee: ${txn.id}, net: ${txn.net / 100}`);
      } else if (txn.type === 'application_fee_refund') {
        // Application fee refunds reduce net volume
        netVolume += txn.net;
        console.log(`[STRIPE DEBUG] Subtracted application fee refund: ${txn.id}, net: ${txn.net / 100}`);
      } else if (txn.type === 'transfer') {
        // Only subtract transfers to connected accounts (vendor payouts)
        // Transfers to external bank accounts (payouts) should NOT be subtracted
        const source = txn.source as any;
        const isConnectedAccountTransfer = source && 
          typeof source === 'object' && 
          source.object === 'transfer' && 
          source.destination &&
          typeof source.destination === 'string' &&
          source.destination.startsWith('acct_');
        
        if (isConnectedAccountTransfer) {
          // This is a transfer to a connected account (vendor payout) - subtract from Net Volume
          netVolume += txn.net;
          console.log(`[STRIPE DEBUG] Excluded connected account transfer: ${txn.id}, amount: ${txn.amount / 100}, destination: ${source.destination}`);
        } else {
          // This is NOT a connected account transfer (e.g., internal or other type)
          // Log it for debugging - don't modify netVolume
          console.log(`[STRIPE DEBUG] Non-connected transfer (NOT modifying Net Volume): ${txn.id}, type: ${source?.object}, amount: ${txn.amount / 100}, net: ${txn.net / 100}, destination: ${source?.destination || 'none'}`);
        }
      } else if (txn.type === 'payout') {
        // Payouts to external bank accounts should NOT affect Net Volume
        // Net Volume represents money earned, not what's currently in Stripe balance
        // We explicitly ignore payouts - they're just moving money you already earned
        console.log(`[STRIPE DEBUG] Ignoring payout to bank: ${txn.id}, amount: ${txn.amount / 100}`);
      } else if (txn.type === 'payment_failure_refund' || txn.type === 'contribution' || 
                 txn.type === 'reserve_transaction' || txn.type === 'reserved_funds' ||
                 txn.type === 'connect_collection_transfer' || txn.type === 'issuing_authorization_hold' ||
                 txn.type === 'issuing_authorization_release' || txn.type === 'issuing_transaction') {
        // These affect net volume
        netVolume += txn.net;
        console.log(`[STRIPE DEBUG] Added ${txn.type}: ${txn.id}, net: ${txn.net / 100}`);
      } else if (txn.type === 'payout_cancel' || txn.type === 'payout_failure') {
        // Payout cancellations/failures add money back (ignore for net volume calculation)
        console.log(`[STRIPE DEBUG] Ignoring ${txn.type}: ${txn.id}, amount: ${txn.amount / 100}`);
      } else {
        // Log any unhandled transaction types for debugging
        console.log(`[STRIPE DEBUG] Unhandled transaction type: ${txn.type}, id: ${txn.id}, amount: ${txn.amount / 100}, net: ${txn.net / 100}`);
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
