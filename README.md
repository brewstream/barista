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

brew.status();                                   // every leg: state, traffic, TS health (Grind)
barista.activate(brew.id(), new SourceId("backup"));
barista.update(brew.spec().withOutputs(...));    // outputs join and leave while it runs
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
```
