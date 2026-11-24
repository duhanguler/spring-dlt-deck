package io.github.duhanguler.dltdeck.core.policy;

import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * HMAC-SHA256 signed keyset pagination cursor codec preventing client-side cursor forgery or SQL seek abuse.
 */
public final class KeysetCursorCodec {

    private static final String HMAC_ALG = "HmacSHA256";
    private final byte[] secretKey;

    public KeysetCursorCodec(byte[] secretKey) {
        if (secretKey.length < 16) {
            throw new IllegalArgumentException("secretKey must be at least 16 bytes");
        }
        this.secretKey = secretKey.clone();
    }

    public String encode(Instant ingestedAt, UUID id) {
        Instant normalized = ingestedAt.truncatedTo(ChronoUnit.MICROS);
        long epochSeconds = normalized.getEpochSecond();
        int nanos = normalized.getNano();
        String payload = epochSeconds + ":" + nanos + ":" + id;
        byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);
        byte[] sig = hmac(payloadBytes);
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        return enc.encodeToString(payloadBytes) + "." + enc.encodeToString(sig);
    }

    public CursorToken decode(String cursor) {
        int dot = cursor.indexOf('.');
        if (dot <= 0 || dot == cursor.length() - 1 || cursor.indexOf('.', dot + 1) >= 0) {
            throw new ValidationDomainException("INVALID_CURSOR", "Malformed keyset pagination cursor");
        }
        Base64.Decoder dec = Base64.getUrlDecoder();
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String payloadPart = cursor.substring(0, dot);
        String sigPart = cursor.substring(dot + 1);
        byte[] payloadBytes;
        byte[] providedSig;
        try {
            payloadBytes = dec.decode(payloadPart);
            providedSig = dec.decode(sigPart);
        } catch (IllegalArgumentException ex) {
            throw new ValidationDomainException("INVALID_CURSOR", "Cursor encoding is invalid", ex);
        }
        if (!enc.encodeToString(payloadBytes).equals(payloadPart)
                || !enc.encodeToString(providedSig).equals(sigPart)) {
            throw new ValidationDomainException("INVALID_CURSOR", "Non-canonical cursor Base64url encoding rejected");
        }
        byte[] expectedSig = hmac(payloadBytes);
        if (!MessageDigest.isEqual(expectedSig, providedSig)) {
            throw new ValidationDomainException("INVALID_CURSOR", "Cursor HMAC signature verification failed");
        }
        String payload = new String(payloadBytes, StandardCharsets.UTF_8);
        String[] parts = payload.split(":", 3);
        if (parts.length != 3) {
            throw new ValidationDomainException("INVALID_CURSOR", "Cursor payload structure is invalid");
        }
        try {
            long epochSeconds = Long.parseLong(parts[0]);
            int nanos = Integer.parseInt(parts[1]);
            UUID id = UUID.fromString(parts[2]);
            return new CursorToken(Instant.ofEpochSecond(epochSeconds, nanos), id);
        } catch (RuntimeException ex) {
            throw new ValidationDomainException("INVALID_CURSOR", "Cursor timestamp or UUID is invalid", ex);
        }
    }

    private byte[] hmac(byte[] data) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALG);
            mac.init(new SecretKeySpec(secretKey, HMAC_ALG));
            return mac.doFinal(data);
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException("HmacSHA256 unavailable", ex);
        }
    }

    public record CursorToken(Instant ingestedAt, UUID id) {}
}
