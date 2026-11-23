package utb.fai;
 
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
 
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App {

	private static final int DEFAULT_DEPTH = 2;
	private static final int TOP_N = 20;
	private static final int TIMEOUT_MS = 10_000;
	private static final int MAX_BODY_BYTES = 5 * 1024 * 1024;
	private static final long GLOBAL_TIMEOUT_SECONDS = 120;
	private static final int MAX_PAGES = 500;
	private static final Pattern WORD = Pattern.compile("\\p{L}+");

	private final int maxDepth;
	private final int debugLevel;
	private final ExecutorService executor;

	private final ConcurrentHashMap<URI, Boolean> visitedURIs = new ConcurrentHashMap<>();
	
	private final List<URIinfo> foundURIs = Collections.synchronizedList(new ArrayList<>());

	private final ConcurrentHashMap<String, LongAdder> wordCounts = new ConcurrentHashMap<>();

	private final AtomicInteger pending = new AtomicInteger();
	private final CountDownLatch done = new CountDownLatch(1);

	public App(int maxDepth, int debugLevel) {
		this.maxDepth = maxDepth;
		this.debugLevel = debugLevel;
		int threads = Math.max(4, Runtime.getRuntime().availableProcessors() * 4); 
		this.executor = Executors.newFixedThreadPool(threads);
	}
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Missing parameter - start URL");
			return;
		}

		int maxDepth = parseInt(args, 1, DEFAULT_DEPTH, "hloubka");
		int debugLevel = parseInt(args, 2, 0, "debugLevel");

		try {
			new App(Math.max(0, maxDepth), debugLevel).crawl(args[0]);
		} catch (Exception e) {
			System.err.println("Zachycena neočekávaná výjimka, končíme...");
			e.printStackTrace();
		}
	}

	private static int parseInt(String[] args, int index, int defaultValue, String name) {
		if (args.length <= index) {
			return defaultValue;
		}
		try {
			return Integer.parseInt(args[index].trim());
		} catch (NumberFormatException e) {
			System.err.println("Neplatný parametr " + name + ", použiji " + defaultValue);
			return defaultValue;
		}
	}

	public void crawl(String startUrl) {
		URI start = normalize(startUrl);
		if (start == null) {
			System.err.println("Neplatná URL: " + startUrl);
		} else {
			submit(new URIinfo(start, 0));
			try {
				if (!done.await(GLOBAL_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
					System.err.println("Vypršel globální limit, výsledek je částečný.");
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		executor.shutdownNow();
		try {
			executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		printResult();
	}

	private void submit(URIinfo info) {
		if (visitedURIs.size() >= MAX_PAGES) {
			return;
		}
		if (visitedURIs.putIfAbsent(info.uri, Boolean.TRUE) != null) {
			return; 
		}
		pending.incrementAndGet();
		try {
			executor.execute(() -> {
				try {
					process(info);
				} catch (Exception e) {
					System.err.println("Chyba při zpracování " + info.uri + ": " + e.getMessage());
				} finally {
					if (pending.decrementAndGet() == 0) {
						done.countDown();
					}
				}
			});
		} catch (RuntimeException e) { 
			if (pending.decrementAndGet() == 0) {
				done.countDown();
			}
		}
	}

	private void process(URIinfo info) throws Exception {
		if (debugLevel > 0) {
			System.err.println("Analyzing " + info.uri + " (depth " + info.depth + ", "
					+ Thread.currentThread().getName() + ")");
		}
		Connection.Response response = Jsoup.connect(info.uri.toString())
				.userAgent("Mozilla/5.0 (compatible; SchoolWebCrawler/1.0)")
				.timeout(TIMEOUT_MS)
				.maxBodySize(MAX_BODY_BYTES)
				.followRedirects(true)
				.ignoreContentType(true)
				.ignoreHttpErrors(true)
				.execute();

		String contentType = response.contentType();
		if (response.statusCode() >= 400
				|| contentType == null
				|| !(contentType.startsWith("text/html") || contentType.startsWith("application/xhtml"))) {
			return;
		}

		Document doc = response.parse();

		countWords(doc.body() != null ? doc.body().text() : doc.text());

		if (info.depth < maxDepth) {
			for (Element a : doc.select("a[href], frame[src]")) {
				String raw = a.hasAttr("href") ? a.absUrl("href") : a.absUrl("src");
				URI link = normalize(raw);
				if (link != null) {
					foundURIs.add(new URIinfo(link, info.depth + 1));
					if (debugLevel > 1) {
						System.err.println("Adding URI: " + link);
					}
					submit(new URIinfo(link, info.depth + 1));
				}
			}
		}
	}

	private void countWords(String text) {
		Matcher m = WORD.matcher(text);
		while (m.find()) {
			String word = m.group().toLowerCase(Locale.ROOT);
			wordCounts.computeIfAbsent(word, k -> new LongAdder()).increment();
		}
	}

	private static URI normalize(String raw) {
		if (raw == null || raw.isBlank()) {
			return null;
		}
		try {
			URI uri = new URI(raw.trim());
			String scheme = uri.getScheme();
			if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))
					|| uri.getHost() == null) {
				return null;
			}
			String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
			StringBuilder sb = new StringBuilder();
			sb.append(scheme.toLowerCase(Locale.ROOT)).append("://");
			if (uri.getRawUserInfo() != null) {
				sb.append(uri.getRawUserInfo()).append('@');
			}
			sb.append(uri.getHost().toLowerCase(Locale.ROOT));
			if (uri.getPort() != -1) {
				sb.append(':').append(uri.getPort());
			}
			sb.append(path);
			if (uri.getRawQuery() != null) {
				sb.append('?').append(uri.getRawQuery());
			}
			return new URI(sb.toString());
		} catch (Exception e) {
			return null;
		}
	}

	private void printResult() {
		System.err.println("Navstiveno stranek: " + visitedURIs.size() + ", nalezeno odkazu: " + foundURIs.size());
		wordCounts.entrySet().stream()
				.sorted(Map.Entry.<String, LongAdder>comparingByValue(Comparator.comparingLong(LongAdder::sum))
						.reversed()
						.thenComparing(Map.Entry.comparingByKey()))
				.limit(TOP_N)
				.forEach(e -> System.out.println(e.getKey() + ";" + e.getValue().sum()));
		System.out.flush();
	}
}

