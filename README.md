# Barista

The relay of **BrewStream**: flows that take a transport stream in over one
protocol and send it out to any number of outputs over any protocol, with
health monitoring on every leg. Built on [Roast](https://github.com/brewstream/roast)
(SRT), [Press](https://github.com/brewstream/press) (RTP) and
[Grind](https://github.com/brewstream/grind) (MPEG-TS analysis).

Barista is not a server. It is beans an application composes:

| Module | |
|---|---|
| `barista-core` | the engine, plain Java |
| `barista-spring-boot-starter` | auto-configuration: engine beans with defaults an application can replace |

**Status:** first release, 0.1.0. Java 21.

```groovy
implementation 'io.github.brewstream:barista-spring-boot-starter:0.1.0'   // Spring Boot
implementation 'io.github.brewstream:barista-core:0.1.0'                  // the engine alone
```

## A brew

A brew is one relay: one or more sources (exactly one active), fanned out to any
number of outputs, over SRT or RTP in any combination.

```java
Brew brew = barista.create(BrewSpec.of("studio-feed",
        List.of(SourceSpec.of("main", 0, SrtListenerEndpoint.any()),          // an encoder publishes here
                SourceSpec.of("backup", 1, RtpReceiveEndpoint.unicast())),    // warm standby
        List.of(OutputSpec.of("playout", RtpSendEndpoint.to("10.0.0.9", 5000).withFec(5, 5)),
                OutputSpec.of("partners", SrtListenerEndpoint.any()))));     // subscribers pull here

brew.status();                                   // every leg: state, a health word (GOOD, DEGRADED,
                                                 // DOWN), traffic, TS health (Grind), SCTE-35
                                                 // markers on each source, and
                                                 // each connection with typed SRT or RTP stats, and
                                                 // what happened and why ("no answer from 10.0.0.9:9000")
brew.keyframe();                                 // latest keyframe of the active source, for a
                                                 // thumbnail decoded in the browser (WebCodecs);
                                                 // extracted only while someone keeps asking
barista.activate(brew.id(), new SourceId("backup"));  // or let the brew switch by itself:
barista.update(brew.spec().withFailover(FailoverPolicy.automatic()));
barista.update(brew.spec().withOutputs(...));    // outputs join and leave while it runs
```

With a failover policy, a brew switches by itself when the active source is lost
(no data for the source-loss timeout) or, for an SRT caller, after failed dials
(3 by default). It picks the healthiest other source by priority, stays at least
the dwell time (10 s by default) on a source, and says why in the event history and
`onSourceActivated`. Failback is off by default: a recovered primary takes over again
only if the policy says so, because switching back on air is a glitch.

A subscriber slower than the stream drops its oldest data and keeps watching.
On an SRT listener output, `withSlowSubscribers(SlowSubscriberPolicy.disconnect())`
disconnects one that stays behind instead (three queues dropped, or 10 s behind,
by default), so it can rejoin at the live edge; the reason is in the history.

SRT listener endpoints of one brew can share a port: give them the same fixed
port and each its own stream ID, and one listener routes every connection to the
endpoint whose stream ID it asks for (an exact match, no naming scheme). Each
endpoint keeps its own passphrase. A shared port is the brew's alone; every stream
on it goes through one socket, and a bind failure fails every endpoint on it.

```java
BrewSpec.of("studio",
        List.of(SourceSpec.of("in", 0, SrtListenerEndpoint.any().withPort(9000).withStreamId("in"))),
        List.of(OutputSpec.of("partners", SrtListenerEndpoint.any().withPort(9000).withStreamId("partners")),
                OutputSpec.of("monitor", SrtListenerEndpoint.any().withPort(9000).withStreamId("monitor"))));
```

Ports left at 0 are allocated when the brew is created and stored with it, so a
restart brings it back on the same ports. Every output has its own queue, so a
slow or dead output never holds up the source or the others.

## With Spring Boot

Add `barista-spring-boot-starter`. It supplies the engine and defaults for
everything pluggable (`BrewRepository`, `PortAllocator`, `NodeIdentity`), and
each default backs off when the application defines its own bean. Brews are
kept in memory unless you provide a `BrewRepository`.

```yaml
barista:
  node:
    id: relay-1                 # stable: stored brews are keyed by it
    published-host: 203.0.113.7 # what callers dial
  ports:
    srt: 9000-9099
    rtp: 5000-5999              # blocks of five (P..P+4), ten apart
  health:
    window: 5s                  # judged once per window
    degraded-loss-percent: 1.0  # transport loss above this is DEGRADED
```

Every source and output carries one health word, decided in one place so every
UI and alert agrees. `DOWN`: not connected, or a source not delivering.
`DEGRADED`: connected, but over the last window transport loss passed the
threshold, the TS on that leg had continuity errors, or an output dropped
chunks from its queue. `GOOD`: otherwise.

### Brews from your own configuration

Barista does not read brews from `application.yml`. If it did, a brew would have
two sources of truth, the configuration and the changes made at runtime through
`Barista.update`, and every restart would need a rule for which one wins. Your
application owns that rule, and creating brews at startup takes a few lines:
bind your own properties, map them to `BrewSpec`s, and create them once the
application is ready.

```yaml
relay:
  brews:
    - id: studio-feed           # a BrewId: up to 64 letters, digits, '.', '_', '-'
      name: Studio feed
      srt-port: 9000            # the encoder publishes here
      outputs:
        - { id: playout, host: 10.0.0.9, port: 5000 }
```

```java
@ConfigurationProperties("relay")
public record RelayProperties(List<ConfiguredBrew> brews) {

    public record ConfiguredBrew(String id, String name, int srtPort, List<Destination> outputs) {
        public BrewSpec toSpec() {
            List<OutputSpec> rtp = outputs.stream()
                    .map(out -> OutputSpec.of(out.id(), RtpSendEndpoint.to(out.host(), out.port())))
                    .toList();
            return new BrewSpec(new BrewId(id), name,
                    List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any().withPort(srtPort))), rtp, true);
        }
    }

    public record Destination(String id, String host, int port) {
    }
}

@Component
public class ConfiguredBrews {

    private static final Logger LOG = Logger.getLogger(ConfiguredBrews.class.getName());
    private final Barista barista;
    private final RelayProperties relay;

    public ConfiguredBrews(Barista barista, RelayProperties relay) {
        this.barista = barista;
        this.relay = relay;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void createConfiguredBrews() {
        for (RelayProperties.ConfiguredBrew configured : relay.brews()) {
            try {
                if (barista.brew(new BrewId(configured.id())).isPresent()) {
                    LOG.info("brew " + configured.id() + " restored from the repository; configuration not applied");
                    continue;
                }
                barista.create(configured.toSpec());
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "brew " + configured.id() + " not created: " + e.getMessage(), e);
            }
        }
    }
}
```

Enable the properties with `@EnableConfigurationProperties(RelayProperties.class)`
or `@ConfigurationPropertiesScan`. The same code, with guards for omitted lists,
is compiled and tested in the starter's tests (`org.brewstream.barista.example`).

**Failures do not stop the application.** A brew whose port is taken, or whose
spec is invalid, is logged and skipped, and the other brews are created. Stopping
the whole relay because one feed is misconfigured would take every other feed
off air.

**With a persistent `BrewRepository`, match by id.** The starter restores the
node's stored brews when the context starts, which is before
`ApplicationReadyEvent`. So by the time this runs, a configured brew may already
exist, restored with whatever was changed at runtime. Brews are matched by
`BrewId`, and the example leaves a restored brew alone: runtime changes win, and
the configuration only seeds brews that do not exist yet. If you want the
configuration to win instead, call `barista.update(configured.toSpec())` for a
brew that already exists. The stored ports are kept for listening endpoints left
at port 0. Either way, removing a brew from the configuration does not delete it.
Call `barista.delete` for brews that are stored but no longer configured, if
that is the rule you want. With the default in-memory repository nothing is
restored, so every configured brew is simply created.

