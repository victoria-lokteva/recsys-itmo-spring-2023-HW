from collections import defaultdict
from .recommender import Recommender
from .random import Random
import random
import heapq


class ExcludeRecommender(Recommender):

    def __init__(self, tracks_redis, recommendations_redis, catalog, artists_timeout, timeout, time_thrs):
        self.tracks_redis = tracks_redis
        self.recommendations_redis = recommendations_redis
        self.fallback = Random(tracks_redis)
        self.catalog = catalog
        self.artists_timeout = artists_timeout
        self.timeout = timeout
        self.time_thrs = time_thrs
        self.recommendations_dict = defaultdict(list)
        self.exclude_artists = defaultdict(list)
        self.exclude_tracks = defaultdict(list)

    def recommend_next(self, user: int, prev_track: int, prev_track_time: float) -> int:
        track_data = self.tracks_redis.get(prev_track)
        if track_data is None:
            return self.fallback.recommend_next(user, prev_track, prev_track_time)

        if len(self.recommendations_dict[user]) == 0:
            heapq.heappush(self.recommendations_dict[user], (-prev_track_time, prev_track))

        track = self.catalog.from_bytes(track_data)
        if track is None:
            return self.fallback.recommend_next(user, prev_track, prev_track_time)

        # if previous recommendation is not successful
        if prev_track_time < self.time_thrs:
            self.exclude_artists[user].append(track.artist)
            if len(self.exclude_artists[user]) > self.artists_timeout:
                self.exclude_artists[user] = self.exclude_artists[user][1:]

            self.exclude_tracks[user].append(track.track)
            if len(self.exclude_tracks[user]) > self.timeout:
                self.exclude_tracks[user] = self.exclude_tracks[user][1:]

            if user in self.recommendations_dict:
                while len(self.recommendations_dict[user]) and self.check_excluded(user, track):
                    prev_track_time, prev_track = heapq.heappop(self.recommendations_dict[user])
                    track_data = self.tracks_redis.get(prev_track)
                    track = self.catalog.from_bytes(track_data)

        recommendations = track.recommendations
        if recommendations is None:
            return self.fallback.recommend_next(user, prev_track, prev_track_time)

        shuffled = list(recommendations)
        random.shuffle(shuffled)

        for next_track in shuffled:
            track_data = self.tracks_redis.get(next_track)
            track = self.catalog.from_bytes(track_data)
            if not self.check_excluded(user, track):
                return next_track

        return self.fallback.recommend_next(user, prev_track, prev_track_time)

    def check_excluded(self, user: int, track):
        return track.artist in self.exclude_artists[user] or track.track in self.exclude_tracks[user]
