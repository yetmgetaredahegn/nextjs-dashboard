import { neon } from '@neondatabase/serverless';
import {
  CustomerField,
  CustomersTableType,
  InvoiceForm,
  InvoicesTable,
  LatestInvoiceRaw,
  Revenue,
} from './definitions';
import { formatCurrency } from './utils';

const databaseUrl = process.env.POSTGRES_URL ?? process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error('Missing POSTGRES_URL or DATABASE_URL environment variable.');
}

const sql = neon(databaseUrl);

const MAX_DB_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 250;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function collectErrorCodes(error: unknown): Set<string> {
  const codes = new Set<string>();
  const stack: unknown[] = [error];
  const visited = new Set<unknown>();

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current || visited.has(current)) continue;
    visited.add(current);

    if (typeof current === 'object') {
      const record = current as {
        code?: unknown;
        cause?: unknown;
        sourceError?: unknown;
        errors?: unknown;
      };

      if (typeof record.code === 'string') {
        codes.add(record.code);
      }

      if (record.cause) stack.push(record.cause);
      if (record.sourceError) stack.push(record.sourceError);

      if (Array.isArray(record.errors)) {
        for (const nested of record.errors) stack.push(nested);
      }
    }
  }

  return codes;
}

function isRetryableConnectionError(error: unknown) {
  const retryableCodes = new Set([
    'ETIMEDOUT',
    'ECONNRESET',
    'ENETUNREACH',
    'EAI_AGAIN',
    'ECONNREFUSED',
    'UND_ERR_CONNECT_TIMEOUT',
  ]);

  const foundCodes = collectErrorCodes(error);
  for (const code of foundCodes) {
    if (retryableCodes.has(code)) return true;
  }

  return false;
}

async function withDbRetry<T>(label: string, operation: () => Promise<T>) {
  let lastError: unknown;

  for (let attempt = 1; attempt <= MAX_DB_RETRIES; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;

      if (!isRetryableConnectionError(error) || attempt === MAX_DB_RETRIES) {
        throw error;
      }

      const delayMs = RETRY_BASE_DELAY_MS * 2 ** (attempt - 1);
      console.warn(
        `[db-retry] ${label} attempt ${attempt} failed, retrying in ${delayMs}ms`,
      );
      await sleep(delayMs);
    }
  }

  throw lastError;
}

export async function fetchRevenue() {
  try {
    // Artificially delay a response for demo purposes.
    // Don't do this in production :)

    // console.log('Fetching revenue data...');
    // await new Promise((resolve) => setTimeout(resolve, 3000));

    const data = (await withDbRetry('fetchRevenue', () =>
      sql`SELECT * FROM revenue`,
    )) as Revenue[];

    // console.log('Data fetch completed after 3 seconds.');

    return data;
  } catch (error) {
    console.error('Database Error:', error);
    if (isRetryableConnectionError(error)) {
      console.warn('[db-fallback] Returning empty revenue data due to connection timeout.');
      return [];
    }
    throw new Error('Failed to fetch revenue data.', { cause: error });
  }
}

export async function fetchLatestInvoices() {
  try {
    const data = (await withDbRetry('fetchLatestInvoices', () =>
      sql`
        SELECT invoices.amount, customers.name, customers.image_url, customers.email, invoices.id
        FROM invoices
        JOIN customers ON invoices.customer_id = customers.id
        ORDER BY invoices.date DESC
        LIMIT 5`,
    )) as LatestInvoiceRaw[];

    const latestInvoices = data.map((invoice) => ({
      ...invoice,
      amount: formatCurrency(invoice.amount),
    }));
    return latestInvoices;
  } catch (error) {
    console.error('Database Error:', error);
    if (isRetryableConnectionError(error)) {
      console.warn('[db-fallback] Returning empty latest invoices due to connection timeout.');
      return [];
    }
    throw new Error('Failed to fetch the latest invoices.', { cause: error });
  }
}

export async function fetchCardData() {
  try {
    // You can probably combine these into a single SQL query
    // However, we are intentionally splitting them to demonstrate
    // how to initialize multiple queries in parallel with JS.
    const invoiceCountPromise = withDbRetry('fetchCardData.invoiceCount', () =>
      sql`SELECT COUNT(*) FROM invoices`,
    );
    const customerCountPromise = withDbRetry('fetchCardData.customerCount', () =>
      sql`SELECT COUNT(*) FROM customers`,
    );
    const invoiceStatusPromise = withDbRetry('fetchCardData.invoiceStatus', () =>
      sql`SELECT
           SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS "paid",
           SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) AS "pending"
           FROM invoices`,
    );

    const data = await Promise.all([
      invoiceCountPromise,
      customerCountPromise,
      invoiceStatusPromise,
    ]);

    const numberOfInvoices = Number(data[0][0].count ?? '0');
    const numberOfCustomers = Number(data[1][0].count ?? '0');
    const totalPaidInvoices = formatCurrency(data[2][0].paid ?? '0');
    const totalPendingInvoices = formatCurrency(data[2][0].pending ?? '0');

    return {
      numberOfCustomers,
      numberOfInvoices,
      totalPaidInvoices,
      totalPendingInvoices,
    };
  } catch (error) {
    console.error('Database Error:', error);
    if (isRetryableConnectionError(error)) {
      console.warn('[db-fallback] Returning zeroed card data due to connection timeout.');
      return {
        numberOfCustomers: 0,
        numberOfInvoices: 0,
        totalPaidInvoices: formatCurrency(0),
        totalPendingInvoices: formatCurrency(0),
      };
    }
    throw new Error('Failed to fetch card data.', { cause: error });
  }
}

const ITEMS_PER_PAGE = 6;
export async function fetchFilteredInvoices(
  query: string,
  currentPage: number,
) {
  const offset = (currentPage - 1) * ITEMS_PER_PAGE;

  try {
    const invoices = (await withDbRetry('fetchFilteredInvoices', () =>
      sql`
        SELECT
          invoices.id,
          invoices.amount,
          invoices.date,
          invoices.status,
          customers.name,
          customers.email,
          customers.image_url
        FROM invoices
        JOIN customers ON invoices.customer_id = customers.id
        WHERE
          customers.name ILIKE ${`%${query}%`} OR
          customers.email ILIKE ${`%${query}%`} OR
          invoices.amount::text ILIKE ${`%${query}%`} OR
          invoices.date::text ILIKE ${`%${query}%`} OR
          invoices.status ILIKE ${`%${query}%`}
        ORDER BY invoices.date DESC
        LIMIT ${ITEMS_PER_PAGE} OFFSET ${offset}
      `,
    )) as InvoicesTable[];

    return invoices;
  } catch (error) {
    console.error('Database Error:', error);
    if (isRetryableConnectionError(error)) {
      console.warn('[db-fallback] Returning empty invoices list due to connection timeout.');
      return [];
    }
    throw new Error('Failed to fetch invoices.', { cause: error });
  }
}

export async function fetchInvoicesPages(query: string) {
  try {
    const data = await withDbRetry('fetchInvoicesPages', () =>
      sql`SELECT COUNT(*)
      FROM invoices
      JOIN customers ON invoices.customer_id = customers.id
      WHERE
        customers.name ILIKE ${`%${query}%`} OR
        customers.email ILIKE ${`%${query}%`} OR
        invoices.amount::text ILIKE ${`%${query}%`} OR
        invoices.date::text ILIKE ${`%${query}%`} OR
        invoices.status ILIKE ${`%${query}%`}
    `,
    );

    const totalPages = Math.ceil(Number(data[0].count) / ITEMS_PER_PAGE);
    return totalPages;
  } catch (error) {
    console.error('Database Error:', error);
    if (isRetryableConnectionError(error)) {
      console.warn('[db-fallback] Returning zero invoice pages due to connection timeout.');
      return 0;
    }
    throw new Error('Failed to fetch total number of invoices.', { cause: error });
  }
}

export async function fetchInvoiceById(id: string) {
  try {
    const data = (await withDbRetry('fetchInvoiceById', () =>
      sql`
        SELECT
          invoices.id,
          invoices.customer_id,
          invoices.amount,
          invoices.status
        FROM invoices
        WHERE invoices.id = ${id};
      `,
    )) as InvoiceForm[];

    const invoice = data.map((invoice) => ({
      ...invoice,
      // Convert amount from cents to dollars
      amount: invoice.amount / 100,
    }));

    return invoice[0];
  } catch (error) {
    console.error('Database Error:', error);
    if (isRetryableConnectionError(error)) {
      console.warn('[db-fallback] Returning empty invoice due to connection timeout.');
      return undefined;
    }
    throw new Error('Failed to fetch invoice.', { cause: error });
  }
}

export async function fetchCustomers() {
  try {
    const customers = (await withDbRetry('fetchCustomers', () =>
      sql`
        SELECT
          id,
          name
        FROM customers
        ORDER BY name ASC
      `,
    )) as CustomerField[];

    return customers;
  } catch (err) {
    console.error('Database Error:', err);
    if (isRetryableConnectionError(err)) {
      console.warn('[db-fallback] Returning empty customers due to connection timeout.');
      return [];
    }
    throw new Error('Failed to fetch all customers.', { cause: err });
  }
}

export async function fetchFilteredCustomers(query: string) {
  try {
    const data = (await withDbRetry('fetchFilteredCustomers', () =>
      sql`
      SELECT
        customers.id,
        customers.name,
        customers.email,
        customers.image_url,
        COUNT(invoices.id) AS total_invoices,
        SUM(CASE WHEN invoices.status = 'pending' THEN invoices.amount ELSE 0 END) AS total_pending,
        SUM(CASE WHEN invoices.status = 'paid' THEN invoices.amount ELSE 0 END) AS total_paid
      FROM customers
      LEFT JOIN invoices ON customers.id = invoices.customer_id
      WHERE
        customers.name ILIKE ${`%${query}%`} OR
          customers.email ILIKE ${`%${query}%`}
      GROUP BY customers.id, customers.name, customers.email, customers.image_url
      ORDER BY customers.name ASC
      `,
    )) as CustomersTableType[];

    const customers = data.map((customer) => ({
      ...customer,
      total_pending: formatCurrency(customer.total_pending),
      total_paid: formatCurrency(customer.total_paid),
    }));

    return customers;
  } catch (err) {
    console.error('Database Error:', err);
    if (isRetryableConnectionError(err)) {
      console.warn('[db-fallback] Returning empty customer table due to connection timeout.');
      return [];
    }
    throw new Error('Failed to fetch customer table.', { cause: err });
  }
}
